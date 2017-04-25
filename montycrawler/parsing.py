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

from html.parser import HTMLParser


class SimpleParser(HTMLParser):
    """Extracts all links and the title from HTML source."""
    def __init__(self, *args, **kwargs):
        """Initializes an empty parser

        Note:
            Arguments are present for compatibility with other parsers, but ignored.

        Args:
            *args: Ignored.
            **kwargs: Ignored.
        """
        super().__init__()
        self.links = None
        self.current = None
        self.title = None
        self.disallowed = False

    def parse(self, text):
        """Run parse process.

        Note:
            This process isn't multithread safe on the same instance.

        Args:
            text: HTML source to be parsed.

        Returns:
            Tuple of:
                Title of page (str)
                List of tuples of:
                    Link (str)
                    Text of link (str)
                    Priority (None)

        """
        self.title = None
        self.disallowed = None
        self.links = []
        self.feed(text)
        # Don't return anything if meta tags prevent from indexing or following
        if self.disallowed:
            return None, []
        else:
            return self.title, self.links

    def handle_starttag(self, tag, attrs):
        """Hook function for start tags.

        Args:
            tag: HTML tag (str)
            attrs: Tag attributes (list of tuples of str and str)

        """
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self.current = (value,)
                elif name == 'rel' and value == 'nofollow':
                    self.current = None
        elif tag == 'title':
                self.title = '_empty_'
        elif tag == 'meta':
            robots = False
            noindex = False
            for name, value in attrs:
                if name == 'name' and value == 'robots':
                    robots = True
                if name == 'content' and ('noindex' in value or 'nofollow' in value):
                    noindex = True
            if robots and noindex:
                self.disallowed = True

    def handle_data(self, data):
        """Hook function for HTML data between tags.

        Args:
            data: Text between tags (str)

        """
        if self.current:
            self.current += (data,)
        if self.title == '_empty_':
            self.title = data

    def handle_endtag(self, tag):
        """Hook function for end tags

        Args:
            tag: HTML tag (str)

        """
        if tag == 'a' and self.current:
            # Force tuples of 2 elements
            if len(self.current) != 2:
                self.current = (self.current[0], None)
            # Placeholder for priority.
            # This simple parser doesn't assign it.
            self.links.append(self.current + (None,))
        self.current = None
        if tag == 'title' and self.title == '_empty_':
            self.title = None

