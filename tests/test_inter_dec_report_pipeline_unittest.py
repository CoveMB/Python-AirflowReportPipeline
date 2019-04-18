import unittest
from airflow.models import DagBag


class TestInterDecReportDAG(unittest.TestCase):
    """Check InterDecReportDAG expectation"""

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        """Check task count of the inter_dec_report dag"""
        dag_id = 'inter_dec_report'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 13)

    def test_contain_tasks(self):
        """Check task contains in inter_dec_report dag"""
        dag_id = 'inter_dec_report'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, [
            'report_init_task',
            'fetching_facebook_data_task',
            'cleanning_facebook_data_task',
            'load_data_bigquery_task',
            'calculatinfg_facebook_data_for_report_task',
            'query_bq_google_ads_task',
            'calculating_google_ads_network_task',
            'calculating_google_ads_search_task',
            'getting_leads_data_task',
            'calculating_leads_last_week_task',
            'calculating_event_leads_task',
            'send_report_email_task',
            'delete_xcom_task'])


suite = unittest.TestLoader().loadTestsFromTestCase(TestInterDecReportDAG)
unittest.TextTestRunner(verbosity=2).run(suite)
