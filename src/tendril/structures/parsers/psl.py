

import csv

from tendril.validation.base import ValidatableBase
from tendril.validation.columns import RequiredColumnMissingError
from tendril.validation.columns import ColumnsRequiredPolicy

from tendril.entities.base import GenericEntity
from tendril.structures.containers import BasicContainer
from tendril.entities.base import EntityHasNoStructure

from tendril.utils import log
logger = log.get_logger(__name__, level=log.DEFAULT)


class MetadataParseException(Exception):
    pass


class PslParserCSV(ValidatableBase):
    _delimiter = ','

    _meta = ["CCODE", "VERSION", "COACHNAME"]
    _owner_ident_name = ["CCODE", "VERSION"]
    _owner_desc_name = ["COACHNAME"]

    _expected_columns = ["Page", "PsNo", "Lv", "DrawingNo", "Alt", "Item",
                         "SDrawingNo", "SAlt", "Description", "St", "QPC"]

    _ident_name = [("SDrawingNo", "SAlt"), ("Description", )]
    _parent_ident_name = [("DrawingNo", "Alt")]

    _qty_name = "QPC"
    _refdes_name = "Item"
    _desc_name = "Description"

    _type_name = "St"
    _type_assembly = "AS"
    _type_part = "CO"

    _handle_qty = False

    def __init__(self, psl_path, vctx=None):
        self._psl_path = psl_path
        self._meta_data = {}
        self._columns = []
        self._active_parents = {}
        super(PslParserCSV, self).__init__(vctx)
        self._column_policy = ColumnsRequiredPolicy(self._validation_context,
                                                    self._expected_columns)
        self._columns_ok = None

    @property
    def psl_path(self):
        return self._psl_path

    def _get_psl_file(self):
        return open(self._psl_path, 'r')

    def _read_meta_line(self, meta_reader, title):
        rtitle, rvalue = next(meta_reader)
        try:
            assert rtitle == title
        except AssertionError:
            raise MetadataParseException("Found {0}, Expected {1}".format(rtitle, title))
        return rvalue

    def _read_meta(self, psl):
        meta_reader = csv.reader(psl, skipinitialspace=True, quotechar='"', delimiter=",")
        for meta in self._meta:
            self._meta_data[meta] = self._read_meta_line(meta_reader, title=meta)
        self.owner_ident = 'v'.join([self._meta_data[x] for x in self._owner_ident_name])
        self.owner_desc = ''.join([self._meta_data[x] for x in self._owner_desc_name])

    def _check_columns(self, reader):
        self._columns = next(reader)
        try:
            self._column_policy.check(self._columns)
            self._columns_ok = True
        except RequiredColumnMissingError as e:
            self._validation_errors.add(e)
            self._columns_ok = False

    def _create_owner(self):
        self._owner = GenericEntity()
        self._owner.define(ident=self.owner_ident, desc=self.owner_desc, refdes=None)
        self._owner.structure = BasicContainer(owner=self._owner)

    @staticmethod
    def _extract_composite_value(line, options):
        for option in options:
            candidate = ''.join([line[x] for x in option])
            if candidate:
                return candidate

    def _extract_ident(self, line):
        return self._extract_composite_value(line, self._ident_name)

    def _extract_parent_ident(self, line):
        return self._extract_composite_value(line, self._parent_ident_name)

    def _generate_line_entities(self, line):
        line_entities = []
        line_ident = self._extract_ident(line)

        if self._handle_qty:
            qty = int(line[self._qty_name])
        else:
            qty = 1

        for i in range(qty):
            line_entity = GenericEntity()
            refdes = line[self._refdes_name]
            if qty > 1:
                refdes += chr(ord('a') + i)
            line_entity.define(ident=line_ident, desc=line[self._desc_name], refdes=refdes)
            if line[self._type_name] == self._type_assembly:
                line_entity.structure = BasicContainer(owner=line_entity)
            line_entities.append(line_entity)
        return line_entities

    def _insert_line_entities(self, line_entities, parents):
        per_parent = int(len(line_entities) / len(parents))

        assert len(parents) * per_parent == len(line_entities)

        for pidx, parent in enumerate(parents):
            for eidx in range(per_parent):
                entity = line_entities[pidx * per_parent + eidx]
                try:
                    parent.insert(entity)
                except EntityHasNoStructure:
                    logger.warn("Adding structure to entity {} with status CO".format(parent.ident))
                    parent.structure = BasicContainer(owner=parent)
                    parent.insert(entity)

    def _parse_line(self, line):
        line = dict(zip(self._columns, line))

        parent_ident = self._extract_parent_ident(line)

        level = int(line['Lv'])
        parents = self._active_parents[level - 1]
        if level > 0:
            assert parents[0].ident == parent_ident

        line_entities = self._generate_line_entities(line)
        self._insert_line_entities(line_entities, parents)

        self._active_parents[level] = line_entities

    def parse(self):
        psl = self._get_psl_file()
        self._read_meta(psl)

        reader = csv.reader(psl, skipinitialspace=True, quotechar='"', delimiter=",")
        self._check_columns(reader)

        if not self._columns_ok:
            logger.warn("Expected Columns Not Found in Provided PSL. "
                        "Not Parsing : " + self._psl_path)
            return

        # TODO Consider checking if it exists in the library first,
        #  and if it does, link to the library part
        self._create_owner()
        self._active_parents[-1] = [self._owner]

        for line in reader:
            self._parse_line(line)

        psl.close()
        return self._owner

    def cleanup(self):
        pass


if __name__ == '__main__':
    pslpath = "/home/chintal/orgs/scratch/MD685v08 LHB SCN Shell.csv"

