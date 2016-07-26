from html.parser import HTMLParser

# Copyright 2016 Jose A. Brihuega Parodi <jose.brihuega@uca.es>

# This file is part of Montycrawler.

# Montycrawler is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Montycrawler is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Montycrawler.  If not, see <http://www.gnu.org/licenses/>.
	
class SimpleParser(HTMLParser):
    """Extracts all links from HTML document"""

    def __init__(self):
        super().__init__()
        self.links = None
        self.current = None
        self.title = None

    def parse(self, text):
        """Run parse process. Not multithread safe on the same instance."""
        self.links = []
        self.feed(text)
        return self.title, self.links

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self.current = (value,)
                    break
        else:
            if tag == 'title':
                self.title = '_empty_'

    def handle_data(self, data):
        if self.current:
            self.current += (data,)
        if self.title == '_empty_':
            self.title = data

    def handle_endtag(self, tag):
        if tag == 'a' and self.current:
            # Force tuples of 2 elements
            if len(self.current) != 2:
                self.current = (self.current[0], None)
            self.links.append(self.current)
        self.current = None
        if tag == 'title' and self.title == '_empty_':
            self.title = None


