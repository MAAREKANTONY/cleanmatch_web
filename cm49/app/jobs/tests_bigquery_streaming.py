import csv
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from jobs.tasks import _consolidate_marketsegmenter_ai_results_streaming


class BigQueryStreamingConsolidationTests(TestCase):
    def test_streaming_consolidation_writes_csv_and_flushes_batches(self):
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            ai_csv = tmp / 'ai.csv'
            final_csv = tmp / 'final.csv'
            with ai_csv.open('w', encoding='utf-8-sig', newline='') as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=[
                        'google_place_id', 'segmentation_confidence',
                        'fyre_market_segment_type0', 'fyre_market_segment_type1', 'fyre_market_segment_type2', 'fyre_market_segment_type3',
                        'ai_selected_for_review', 'ai_segment_source', 'ai_segment_suggested',
                    ],
                    delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n'
                )
                writer.writeheader()
                writer.writerow({
                    'google_place_id': '1', 'segmentation_confidence': '0.90',
                    'fyre_market_segment_type0': 'Cafe', 'fyre_market_segment_type1': '', 'fyre_market_segment_type2': '', 'fyre_market_segment_type3': '',
                    'ai_selected_for_review': 'no', 'ai_segment_source': 'rules_initial', 'ai_segment_suggested': '',
                })
                writer.writerow({
                    'google_place_id': '2', 'segmentation_confidence': '0.20',
                    'fyre_market_segment_type0': '', 'fyre_market_segment_type1': '', 'fyre_market_segment_type2': '', 'fyre_market_segment_type3': '',
                    'ai_selected_for_review': 'yes', 'ai_segment_source': 'llm_live', 'ai_segment_suggested': 'Restaurant > Fast Casual',
                })
                writer.writerow({
                    'google_place_id': '3', 'segmentation_confidence': '0.10',
                    'fyre_market_segment_type0': 'Bakery', 'fyre_market_segment_type1': '', 'fyre_market_segment_type2': '', 'fyre_market_segment_type3': '',
                    'ai_selected_for_review': 'no', 'ai_segment_source': 'rules_initial', 'ai_segment_suggested': '',
                })

            batches = []
            stats = _consolidate_marketsegmenter_ai_results_streaming(
                ai_csv_path=ai_csv,
                final_csv_path=final_csv,
                process_id='job-1',
                low_conf_threshold=0.65,
                progress=lambda percent, message: None,
                log=lambda message: None,
                bigquery_batch_callback=lambda rows: batches.append(list(rows)) or len(rows),
                created_at_mode='DATETIME',
                insert_batch_size=2,
                progress_log_every=1,
            )

            self.assertEqual(stats['result_rows'], 3)
            self.assertEqual(stats['bigquery_rows_inserted'], 3)
            self.assertEqual(len(batches), 2)
            self.assertEqual(len(batches[0]), 2)
            self.assertEqual(len(batches[1]), 1)

            with final_csv.open('r', encoding='utf-8-sig', newline='') as fh:
                rows = list(csv.DictReader(fh, delimiter=';'))
            self.assertEqual(rows[0]['market_segment_type0'], 'Cafe')
            self.assertEqual(rows[1]['market_segment_type0'], 'Restaurant')
            self.assertEqual(rows[1]['market_segment_type1'], 'Fast Casual')
            self.assertEqual(rows[2]['market_segment_type0'], 'Bakery')

    def test_streaming_consolidation_keeps_csv_when_bigquery_batch_fails(self):
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            ai_csv = tmp / 'ai.csv'
            final_csv = tmp / 'final.csv'
            with ai_csv.open('w', encoding='utf-8-sig', newline='') as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=['google_place_id', 'segmentation_confidence', 'fyre_market_segment_type0', 'fyre_market_segment_type1', 'fyre_market_segment_type2', 'fyre_market_segment_type3', 'ai_selected_for_review', 'ai_segment_source', 'ai_segment_suggested'],
                    delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n'
                )
                writer.writeheader()
                for idx in range(3):
                    writer.writerow({
                        'google_place_id': str(idx + 1), 'segmentation_confidence': '0.90',
                        'fyre_market_segment_type0': 'Cafe', 'fyre_market_segment_type1': '', 'fyre_market_segment_type2': '', 'fyre_market_segment_type3': '',
                        'ai_selected_for_review': 'no', 'ai_segment_source': 'rules_initial', 'ai_segment_suggested': '',
                    })

            calls = {'n': 0}
            def failing_callback(rows):
                calls['n'] += 1
                if calls['n'] == 1:
                    raise RuntimeError('boom')
                return len(rows)

            stats = _consolidate_marketsegmenter_ai_results_streaming(
                ai_csv_path=ai_csv,
                final_csv_path=final_csv,
                process_id='job-2',
                low_conf_threshold=0.65,
                progress=lambda percent, message: None,
                log=lambda message: None,
                bigquery_batch_callback=failing_callback,
                created_at_mode='DATETIME',
                insert_batch_size=2,
                progress_log_every=10,
            )

            self.assertEqual(stats['result_rows'], 3)
            self.assertEqual(stats['bigquery_rows_inserted'], 0)
            self.assertIn('boom', stats['bigquery_write_error'])
            with final_csv.open('r', encoding='utf-8-sig', newline='') as fh:
                rows = list(csv.DictReader(fh, delimiter=';'))
            self.assertEqual(len(rows), 3)
