"""
AgTalk Five-Year Forum Spider

Description:
    Scrapes thread and post data from the Agtalk forum using 
    bookmark pagination

Data Collected:
    - thread_id
    - thread_title 
    - thread_url 
    - post_id
    - username
    - post_date
    - post_text
    
"""

# Standard Library Imports
import re
import urllib.parse

# Third-Party Imports
import scrapy



# Spider Class for crawling
class AgtalkForumSpider(scrapy.Spider):
    name = "five_year_spider"
    # Website to scrape
    allowed_domains = ["talk.newagtalk.com"]

    def start_requests(self):
       
        # Bookmark-based pagination step size is forum threads per page
        for bookmark in range(0, 110000, 50):
            url = f"https://talk.newagtalk.com/forums/forum-view.asp?fid=2&bookmark={bookmark}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"bookmark": bookmark}
            )

    # Extracting thread links from Forum Page
    def parse(self, response):
        thread_links = response.css("a[href*='thread-view.asp?tid=']::attr(href)").getall()

        for link in thread_links:
            yield response.follow(link, callback=self.parse_thread,
                                  meta={"bookmark": response.meta.get("bookmark")}
                                )
            
    # Extracting Post data from thread page
    def parse_thread(self, response):
        parsed = urllib.parse.urlparse(response.url)
        query = urllib.parse.parse_qs(parsed.query)
        thread_id =  query.get("tid", [None])[0]
        thread_title = response.css("title::text").get()

        posts = response.xpath("//td[@class='messageheader']/parent::tr")

        for post in posts:
            header_text_parts = post.xpath(
                ".//td[@class='messageheader']//span[@class='smalltext']//text()"
            ).getall()
            header_text = " ".join(t.strip() for t in header_text_parts if t.strip())

            date_match = re.search(
                r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}", header_text
            )
            post_date = date_match.group(0) if date_match else None
            
                

            id_match = re.search(r"#(\d+)", header_text)
            post_id = id_match.group(1) if id_match else None

            username = post.css(
                'td.messageheader > a[href*="view-profile"]::text'
            ).get()
            if username:
                username = username.strip()

            body_row = post.xpath("following-sibling::tr[1]")
            body_text_parts = body_row.xpath(".//td//text()").getall()
            post_text = " ".join(t.strip() for t in body_text_parts if t.strip())
            # Yielding structured post record
            yield {
                "thread_id": thread_id,
                "thread_title": thread_title,
                "thread_url": response.url,
                "bookmark": response.meta.get("bookmark"),
                "post_id": post_id,
                "username": username,
                "post_date": post_date,
                "post_text": post_text,
            }
