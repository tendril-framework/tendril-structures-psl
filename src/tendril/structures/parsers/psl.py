

from tendril.validation.base import ValidatableBase


class PslParser(ValidatableBase):
    def __init__(self, psl_path, vctx=None):
        self._psl_path = psl_path
        super(PslParser, self).__init__(vctx)

    @property
    def psl_path(self):
        return self._psl_path

    def parse(self):
        pass


if __name__ == '__main__':
    ident = "MD685v08 LHB SCN Shell"
    pslpath = "~/orgs/scratch/MD685v08 LHB SCN Shell.csv"
