
import unittest
from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):
    """
    Generic tests that all DAGs in the repository should be able to pass.
    """

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        """
        Verify that Airflow will be able to import all DAGs in the repository.
        """
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )


suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)
