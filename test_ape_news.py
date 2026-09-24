import os
import unittest
from unittest.mock import Mock, call, patch

import ape_news


class FetchMarketContextTest(unittest.TestCase):
    def response(self, payload):
        response = Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        return response

    @patch.dict(
        os.environ,
        {
            "CONTEXT_API_URL": "https://context-api.test/v1/context",
            "CONTEXT_API_REQUIRED": "false",
        },
        clear=False,
    )
    @patch("ape_news.time.sleep")
    @patch("ape_news.request_with_retry")
    def test_retries_empty_response_then_returns_warmed_context(self, request, sleep):
        request.side_effect = [
            self.response({}),
            self.response({"AMD": {"price": {"daily_pct": 1.2}}}),
        ]

        result = ape_news.fetch_market_context(Mock(), ["AMD"])

        self.assertEqual({"AMD": {"price": {"daily_pct": 1.2}}}, result)
        self.assertEqual(2, request.call_count)
        sleep.assert_called_once_with(5.0)

    @patch.dict(
        os.environ,
        {
            "CONTEXT_API_URL": "https://context-api.test/v1/context",
            "CONTEXT_API_REQUIRED": "false",
        },
        clear=False,
    )
    @patch("ape_news.time.sleep")
    @patch("ape_news.request_with_retry")
    def test_accepts_partial_non_empty_response_without_retry(self, request, sleep):
        request.return_value = self.response({"META": {"insiders_3m": "net selling"}})

        result = ape_news.fetch_market_context(Mock(), ["META", "UNKNOWN"])

        self.assertEqual({"META": {"insiders_3m": "net selling"}}, result)
        request.assert_called_once()
        sleep.assert_not_called()

    @patch.dict(
        os.environ,
        {
            "CONTEXT_API_URL": "https://context-api.test/v1/context",
            "CONTEXT_API_REQUIRED": "false",
        },
        clear=False,
    )
    @patch("ape_news.time.sleep")
    @patch("ape_news.request_with_retry")
    def test_stops_after_bounded_empty_retries(self, request, sleep):
        request.side_effect = [
            self.response({}), self.response({}), self.response({}), self.response({})
        ]

        result = ape_news.fetch_market_context(Mock(), ["AMD"])

        self.assertEqual({}, result)
        self.assertEqual(4, request.call_count)
        self.assertEqual([call(5.0), call(10.0), call(20.0)], sleep.call_args_list)

    @patch.dict(
        os.environ,
        {
            "CONTEXT_API_URL": "https://context-api.test/v1/context",
            "CONTEXT_API_REQUIRED": "true",
        },
        clear=False,
    )
    @patch("ape_news.time.sleep")
    @patch("ape_news.request_with_retry")
    def test_required_context_aborts_before_publish_when_it_stays_empty(self, request, sleep):
        request.side_effect = [
            self.response({}), self.response({}), self.response({}), self.response({})
        ]

        with self.assertRaisesRegex(RuntimeError, "stayed empty after 4 request"):
            ape_news.fetch_market_context(Mock(), ["AMD"])

        self.assertEqual(4, request.call_count)
        self.assertEqual([call(5.0), call(10.0), call(20.0)], sleep.call_args_list)


if __name__ == "__main__":
    unittest.main()
