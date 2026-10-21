import json
import unittest

from job_queue import Job


class TestJob(unittest.TestCase):
    def test_round_trip_through_json(self):
        job = Job(job_type="send_email", payload={"to": "a@example.com", "retries": 0})
        restored = Job.from_json(job.to_json())
        self.assertEqual(restored, job)

    def test_from_json_accepts_bytes(self):
        # redis-py hands back bytes by default
        job = Job(job_type="resize_image", payload={"width": 200})
        restored = Job.from_json(job.to_json().encode("utf-8"))
        self.assertEqual(restored, job)

    def test_each_job_gets_its_own_id(self):
        ids = {Job(job_type="noop").job_id for _ in range(100)}
        self.assertEqual(len(ids), 100)

    def test_payload_defaults_to_empty_dict(self):
        self.assertEqual(Job(job_type="noop").payload, {})

    def test_empty_job_type_is_rejected(self):
        for bad_type in ["", "   ", None, 5]:
            with self.subTest(job_type=bad_type):
                with self.assertRaises(ValueError):
                    Job(job_type=bad_type)

    def test_payload_must_be_a_dict(self):
        with self.assertRaises(ValueError):
            Job(job_type="noop", payload=["not", "a", "dict"])

    def test_payload_must_be_json_serializable(self):
        # A set can't be turned into JSON, so this should fail now rather than in a worker later.
        with self.assertRaises(ValueError):
            Job(job_type="noop", payload={"ids": {1, 2, 3}})

    def test_from_json_rejects_garbage(self):
        with self.assertRaises(ValueError):
            Job.from_json("this is not json")

    def test_from_json_rejects_missing_fields(self):
        with self.assertRaises(ValueError):
            Job.from_json(json.dumps({"job_type": "noop"}))

    def test_from_json_rejects_non_object(self):
        with self.assertRaises(ValueError):
            Job.from_json(json.dumps([1, 2, 3]))


if __name__ == "__main__":
    unittest.main()
