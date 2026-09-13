import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from bs4 import BeautifulSoup
from generator import generate_pulse as pulse
from generator.check_schedule import should_build


NOW = datetime(2026, 9, 14, 14, 0, tzinfo=timezone.utc)


def article(**changes):
    item = dict(title='Microsoft launches AI software', url='https://example.com/news?x=1&y=2',
                published_at=NOW.isoformat(), source='Example News', category='ai',
                summary_text='New tools for developers.', image_url='https://example.com/image.jpg')
    item.update(changes)
    return item


class RenderingTests(unittest.TestCase):
    def test_dates_categories_and_feed_escaping(self):
        item = article(title='A "quote" <script>alert(1)</script>',
                       image_url='https://example.com/a.jpg" onerror="alert(1)',
                       _abs_permalink='https://example.com/p/" onclick="alert(1)')
        document = BeautifulSoup(pulse.build_section('Sep 14, 2026', {'ai': [item]}, brief={}, generated_at=NOW), 'html.parser')
        card = document.select_one('[data-card]')
        self.assertEqual(card['data-category'], 'ai')
        self.assertEqual(card.select_one('h3').get_text(), item['title'])
        self.assertIsNone(card.select_one('script'))
        self.assertFalse(document.select('[onerror], [onclick]'))
        self.assertIn('Sep 14, 2026', card.select_one('.meta').get_text())
        self.assertEqual(card.select_one('img.article-thumb')['alt'], item['title'])
        self.assertNotIn('{{', str(document))

    def test_unsafe_article_and_brief_links_are_not_rendered(self):
        page = pulse.build_section('Sep 14, 2026', {'ai': [article(url='javascript:alert(1)')]}, brief={}, generated_at=NOW)
        self.assertFalse(BeautifulSoup(page, 'html.parser').select('[data-card]'))
        brief = pulse.render_pulse_brief_html({'takeaways': [dict(title='Unsafe', url='javascript:alert(1)')]})
        self.assertNotIn('javascript:', brief)

    def test_permalink_template_escapes_without_recursive_replacement(self):
        output = pulse._render_template_string('<h1>{{TITLE}}</h1><p>{{SUMMARY}}</p>',
                                               TITLE='<img onerror="x"> {{SUMMARY}}', SUMMARY='safe & sound')
        self.assertIn('&lt;img', output)
        self.assertIn('{{SUMMARY}}</h1>', output)
        self.assertIn('safe &amp; sound', output)

    def test_future_articles_are_not_in_window_or_badged_new(self):
        item = article(published_at=(NOW + timedelta(days=2)).isoformat())
        with patch.object(pulse, 'now_et', return_value=NOW):
            self.assertFalse(pulse.within_window(NOW + timedelta(days=2)))
            self.assertTrue(pulse.within_window(NOW - timedelta(days=1)))
        page = pulse.build_section('Sep 14, 2026', {'ai': [item]}, brief={}, generated_at=NOW)
        self.assertFalse(BeautifulSoup(page, 'html.parser').select('.badge'))

    def test_permalink_output_escapes_feed_content(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(pulse, 'PERMA_ROOT', temporary), \
             patch.object(pulse, 'create_branded_og_image', return_value=('', '', '')):
            item = article(title='<script>alert("test")</script>')
            pulse.write_permalink_page(item)
            output = next(Path(temporary).glob('*/index.html')).read_text(encoding='utf-8')
            page = BeautifulSoup(output, 'html.parser')
            self.assertEqual(page.h1.get_text(), item['title'])
            self.assertIsNone(page.h1.script)
            self.assertEqual(page.select_one('link[rel="canonical"]')['href'], item['_abs_permalink'])


class FetchTests(unittest.TestCase):
    def response(self, content, error=None):
        session = Mock()
        response = Mock(content=content, url='https://example.com/rss', headers={'content-type': 'application/rss+xml'})
        response.raise_for_status.side_effect = error
        session.get.return_value = response
        context = Mock()
        context.__enter__ = Mock(return_value=session)
        context.__exit__ = Mock(return_value=False)
        return session, context

    def test_fetch_uses_timeout_and_parses_bytes(self):
        rss = b'''<rss version="2.0"><channel><title>Example</title><item><title>AI launch</title><link>https://example.com/news</link><pubDate>Mon, 14 Sep 2026 13:00:00 GMT</pubDate><description>Developer tools</description></item></channel></rss>'''
        session, context = self.response(rss)
        with patch.object(pulse, 'create_session', return_value=context), \
             patch.object(pulse, 'now_et', return_value=NOW), \
             patch.object(pulse, 'extract_image_url', return_value='https://example.com/image.jpg'):
            items = pulse.fetch_rss('Example', 'https://example.com/rss')
        self.assertEqual(len(items), 1)
        session.get.assert_called_once_with('https://example.com/rss', timeout=(10, 30))

    def test_fetch_does_not_scrape_article_pages(self):
        # Entries without an image used to trigger a page fetch per entry before
        # filtering; image scraping now happens only for published articles.
        rss = b'''<rss version="2.0"><channel><title>Example</title><item><title>AI launch</title><link>https://example.com/news</link><pubDate>Mon, 14 Sep 2026 13:00:00 GMT</pubDate></item></channel></rss>'''
        _, context = self.response(rss)
        with patch.object(pulse, 'create_session', return_value=context), \
             patch.object(pulse, 'now_et', return_value=NOW), \
             patch.object(pulse, 'fetch_html') as page_fetch:
            items = pulse.fetch_rss('Example', 'https://example.com/rss')
        self.assertEqual(items[0]['image_url'], '')
        page_fetch.assert_not_called()

    def test_http_error_and_non_feed_response_are_failures(self):
        for content, error in [(b'', requests.HTTPError('503')), (b'<html>Blocked</html>', None)]:
            _, context = self.response(content, error)
            with patch.object(pulse, 'create_session', return_value=context):
                with self.assertRaises(RuntimeError):
                    pulse.fetch_rss('Example', 'https://example.com/rss')

    def test_partial_outage_preserves_healthy_feed_and_records_failure(self):
        def fetch(name, url):
            if name == 'Broken':
                raise requests.Timeout()
            return [article()]
        with patch.object(pulse, 'fetch_rss', side_effect=fetch):
            result = pulse.fetch_all_rss_parallel([dict(name='Working', url='https://example.com'), dict(name='Broken', url='https://example.org')])
        self.assertEqual(len(result), 1)
        self.assertEqual({s['source']: s['status'] for s in pulse.FEED_STATUS}, {'Working': 'ok', 'Broken': 'error'})

    def test_empty_build_does_not_overwrite_existing_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            docs = Path(temporary) / 'docs'
            docs.mkdir()
            (docs / 'index.html').write_text('Existing healthy edition', encoding='utf-8')
            (docs / 'pulse.json').write_text('[{"existing":true}]', encoding='utf-8')
            with patch.object(pulse, 'REPO', temporary), patch.object(pulse, 'fetch_all_rss_parallel', return_value=[]):
                with self.assertRaisesRegex(RuntimeError, 'No publishable articles'):
                    pulse.main()
            self.assertEqual((docs / 'index.html').read_text(), 'Existing healthy edition')
            self.assertEqual(json.loads((docs / 'pulse.json').read_text()), [{'existing': True}])

    def test_full_build_keeps_feed_contract_and_generates_matching_pages(self):
        with tempfile.TemporaryDirectory() as temporary:
            docs = Path(temporary) / 'docs'
            with patch.object(pulse, 'REPO', temporary), patch.object(pulse, 'PERMA_ROOT', str(docs / 'p')), \
                 patch.object(pulse, 'now_et', return_value=NOW), \
                 patch.object(pulse, 'fetch_all_rss_parallel', return_value=[article(content_html='<p>raw</p>')]), \
                 patch.object(pulse, 'create_branded_og_image', return_value=('', '', '')):
                pulse.main()
            feed = json.loads((docs / 'pulse.json').read_text(encoding='utf-8'))
            self.assertIsInstance(feed, list)
            self.assertEqual(len(feed), 1)
            self.assertTrue({'title', 'url', 'published_at', 'source', 'category', 'summary_text'}.issubset(feed[0]))
            self.assertNotIn('content_html', feed[0])
            document = BeautifulSoup((docs / 'index.html').read_text(encoding='utf-8'), 'html.parser')
            self.assertEqual(len(document.select('[data-card]')), 1)
            self.assertEqual(len(list((docs / 'p').glob('*/index.html'))), 1)
            status = json.loads((docs / 'build-status.json').read_text())
            self.assertEqual(status['articles'], 1)
            self.assertEqual(status['by_category']['ai'], 1)
            sitemap = (docs / 'sitemap.xml').read_text()
            self.assertIn(feed[0]['_abs_permalink'], sitemap)
            self.assertIn('<lastmod>2026-09-14</lastmod>', sitemap)
            # Archives feed the next run's merge; they carry the public shape too.
            snapshot = next((docs / 'archive' / 'timestamped').glob('*.json'))
            self.assertNotIn('content_html', json.loads(snapshot.read_text(encoding='utf-8'))['items'][0])


class PipelineTests(unittest.TestCase):
    def test_per_source_cap_keeps_newest_and_exempts_house_feed(self):
        items = [article(title=f'Story {i}', url=f'https://example.com/{i}') for i in range(12)]
        items += [article(title=f'House {i}', url=f'https://tek2day.example/{i}', source='TEK2day Newsletter') for i in range(3)]
        capped = pulse.cap_per_source(items, cap=10)
        self.assertEqual([it['title'] for it in capped if it['source'] == 'Example News'], [f'Story {i}' for i in range(10)])
        self.assertEqual(sum(1 for it in capped if it['source'] == 'TEK2day Newsletter'), 3)
        self.assertEqual(len(pulse.cap_per_source(items, cap=None)), 15)

    def test_public_feed_drops_raw_feed_html(self):
        public = pulse.public_item(article(content_html='<p>raw</p>', summary='keep'))
        self.assertNotIn('content_html', public)
        self.assertEqual(public['summary'], 'keep')

    def test_page_fetch_is_memoized_including_failures(self):
        pulse._PAGE_CACHE.clear()
        with patch.object(pulse, 'fetch_html', side_effect=requests.ConnectionError('down')) as fetch:
            self.assertEqual(pulse.fetch_page_cached('https://example.com/a'), '')
            self.assertEqual(pulse.fetch_page_cached('https://example.com/a'), '')
        self.assertEqual(fetch.call_count, 1)
        with patch.object(pulse, 'fetch_html', return_value='<html>ok</html>') as fetch:
            self.assertEqual(pulse.fetch_page_cached('https://example.com/b'), '<html>ok</html>')
            self.assertEqual(pulse.fetch_page_cached('https://example.com/b'), '<html>ok</html>')
        self.assertEqual(fetch.call_count, 1)
        pulse._PAGE_CACHE.clear()

    def test_permalink_reuses_existing_images_and_records_scraped_source(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(pulse, 'PERMA_ROOT', temporary):
            item = article(image_url='')
            with patch.object(pulse, 'create_branded_og_image',
                              return_value=('/p/x/og-image.jpg', '/p/x/thumbnail.jpg', 'https://example.com/scraped.jpg')) as make:
                pulse.write_permalink_page(item)
            make.assert_called_once()
            self.assertEqual(item['image_url'], 'https://example.com/scraped.jpg')
            self.assertTrue(item['_thumbnail'].endswith('/p/x/thumbnail.jpg'))

            perma_dir = next(p for p in Path(temporary).iterdir() if p.is_dir())
            (perma_dir / pulse.OG_IMAGE_NAME).write_bytes(b'jpeg')
            (perma_dir / pulse.THUMBNAIL_NAME).write_bytes(b'jpeg')
            with patch.object(pulse, 'create_branded_og_image') as make:
                pulse.write_permalink_page(dict(item))
            make.assert_not_called()

    def test_sitemap_lastmod_uses_publication_date(self):
        with tempfile.TemporaryDirectory() as temporary:
            docs = Path(temporary)
            (docs / 'p' / 'abcdef0123').mkdir(parents=True)
            (docs / 'p' / 'abcdef0123' / 'index.html').write_text('x', encoding='utf-8')
            with patch.object(pulse, 'now_et', return_value=NOW):
                pulse.write_sitemap(str(docs), lastmod_by_pid={'abcdef0123': '2026-09-01'})
            xml = (docs / 'sitemap.xml').read_text(encoding='utf-8')
            self.assertIn('<loc>https://pulse.tek2dayholdings.com/p/abcdef0123/</loc>', xml)
            self.assertIn('<lastmod>2026-09-01</lastmod>', xml)

    def test_archive_merge_brings_back_recent_items_and_caps_per_source(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(pulse, 'REPO', temporary):
            archive = Path(temporary) / 'docs' / 'archive' / 'timestamped'
            archive.mkdir(parents=True)
            archived = article(title='Archived story', url='https://example.com/archived',
                               published_at=(NOW - timedelta(days=1)).isoformat())
            stale = article(title='Stale story', url='https://example.com/stale',
                            published_at=(NOW - timedelta(days=10)).isoformat())
            (archive / '2026-09-13_120000.json').write_text(json.dumps({'items': [archived, stale]}), encoding='utf-8')
            fresh = [article(title=f'Fresh {i}', url=f'https://example.com/fresh/{i}') for i in range(3)]
            with patch.object(pulse, 'MAX_ITEMS_PER_SOURCE', 3):
                by_cat = pulse.bucket_recent_by_category(fresh, now_local=NOW)
            titles = [it['title'] for it in by_cat['ai']]
            self.assertEqual(titles, ['Fresh 0', 'Fresh 1', 'Fresh 2'])  # cap of 3 drops the older archived story
            with patch.object(pulse, 'MAX_ITEMS_PER_SOURCE', None):
                by_cat = pulse.bucket_recent_by_category(fresh, now_local=NOW)
            titles = [it['title'] for it in by_cat['ai']]
            self.assertIn('Archived story', titles)
            self.assertNotIn('Stale story', titles)


class ScheduleTests(unittest.TestCase):
    def test_weekday_boundary_is_evaluated_in_eastern_time(self):
        # Friday 23:30 Eastern is already Saturday 03:30 UTC: still a weekday build.
        friday_late = datetime(2026, 9, 19, 3, 30, tzinfo=timezone.utc)
        self.assertTrue(should_build('schedule', friday_late, weekdays_only=True))
        # Sunday 21:00 Eastern is Monday 01:00 UTC: still the weekend, skipped.
        sunday_late = datetime(2026, 9, 21, 1, 0, tzinfo=timezone.utc)
        self.assertFalse(should_build('schedule', sunday_late, weekdays_only=True))
        # A slot that GitHub starts late in the evening is not dropped.
        friday_evening = datetime(2026, 9, 18, 23, 45, tzinfo=timezone.utc)
        self.assertTrue(should_build('schedule', friday_evening, weekdays_only=True))

    def test_weekends_skip_and_manual_runs_bypass_gate(self):
        sunday = datetime(2026, 9, 13, 14, tzinfo=timezone.utc)
        self.assertTrue(should_build('schedule', sunday))
        self.assertFalse(should_build('schedule', sunday, weekdays_only=True))
        self.assertTrue(should_build('workflow_dispatch', sunday, weekdays_only=True))


if __name__ == '__main__':
    unittest.main()
