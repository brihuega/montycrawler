from io import BytesIO
from PyPDF2 import PdfFileReader

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


class PDFProcessor:
    """Get metadata and guess relevancy od a PDF document"""
    def __init__(self, keywords=None):
        self.keywords = keywords

    def process(self, content, mimetype='application/pdf'):
        relevancy = 0
        metadata = {}
        if mimetype == 'application/pdf':
            # Obtain metadata
            doc = PdfFileReader(BytesIO(content))
            info = doc.getDocumentInfo()
            if info:
                for k in info:
                    metadata[k] = info.getText(k)
            # Extra metadata
            metadata['_num_pages'] = doc.getNumPages()
            # Process title, subject and metadata keywords
            # TODO guess title from page text when not provided
            relevant = (metadata.get('/Title') + ' ' +
                        metadata.get('/Subject') + ' ' +
                        metadata.get('/Keywords')).lower()
            for word in self.keywords:
                if word.lower() in relevant:
                    # Each relevant keyword increases relevancy in 10 points
                    relevancy += 10
            # Process pages.
            distance_factor = 1
            for p in range(doc.getNumPages()):
                # Break if factor is too low
                if distance_factor < 0.01:
                    break
                text = doc.getPage(p).extractText().lower()
                for word in self.keywords:
                    relevancy += distance_factor * text.count(word.lower())
                # Each new page reduces relevancy factor in a half
                distance_factor /= 2
        # Relevancy is significant by the nearest tenth
        relevancy = round(relevancy, 1)
        metadata['_relevancy'] = relevancy
        return relevancy, metadata
