from collections import Counter
from mrcc import CCJob
from bs4 import BeautifulSoup


class EmailCount(CCJob):
    def process_record(self, record):
        if record['Content-Type'] == 'application/http; msgtype=response':
            try:
                payload = record.payload.read()
                # print "GOT HERE"

                data = record.payload.read()
                headers, body = payload.split('\r\n\r\n', 1)

                soup = BeautifulSoup(body)
                # print soup
                for a in soup.find_all('a', href=True):
                    if 'mailto:' in a['href']:
                        email = a['href'].split('mailto:')[-1]
                        email = email.split('?subject')[0]
                        email = email.split('?Subject')[0]
                        email = email.split('&subject')[0]
                        email = email.split('&Subject')[0]
                        email = email.split('?body')[0]
                        email = email.split('?Body')[0]
                        email = email.split('&body')[0]
                        email = email.split('&Body')[0]
                        if email:
                            email = u''.join(email).encode('utf-8').strip()
                            print "Found the URL:", email
                            yield email, 1
            except:
                print "skipping"

            # self.increment_counter('commoncrawl', 'processed_pages', 1)


if __name__ == '__main__':
    EmailCount.run()
