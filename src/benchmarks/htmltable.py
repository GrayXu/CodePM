#
# Copyright 2017, University of California, San Diego
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in
#       the documentation and/or other materials provided with the
#       distribution.
#
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Highlighting tricks from:
# https://css-tricks.com/simple-css-row-column-highlighting

HTML_HEADER = '''\
<html>
    <head>
'''

HTML_STYLE_HEADER = '''\
        <style>
            table {
                border: 1px solid black;
                border-collapse: collapse;
                overflow: hidden;
            }

            th {
                color: black;
                background-color: #E5E4E2;
            }

            td {
                table-layout: fixed;
                width: 60px
            }

            td, th {
                text-align: right;
                position: relative;
                font-weight: normal;
                white-space: nowrap;
                padding: 0px 5px 0px 5px;
            }

            tr:hover {
                background-color: lightblue;
            }

            td:hover::after,
            th:hover::after {
                content: "";
                position: absolute;
                background-color: lightblue;
                left: 0;
                top: -5000px;
                height: 10000px;
                width: 100%;
                z-index: -1;
            }
'''

HTML_STYLE_SPECIAL = '''
            td:nth-child(3n+1) {
                border-right: 3px solid black;
            }
'''

HTML_STYLE_FOOTER = '''
        </style>
    </head>
    <body>
'''

HTML_TEXT = '''
'''

HTML_FOOTER = '''
    </body>
</html>
'''

def df2html(df, title='dataframe in html', text=HTML_TEXT, style="Regular", filename="dataframe.html"):
    with open(filename, 'w') as html:
        html.write(HTML_HEADER)
        html.write(HTML_STYLE_HEADER)
        if style == "Special":
            html.write(HTML_STYLE_SPECIAL)
        html.write(HTML_STYLE_FOOTER)
        html.write('\n<title>' + title + '</title>\n')
        html.write('\n<br/><b>' + title + '</b><br/><br/>\n')
        html.write(df.to_html())
        html.write(text)
        html.write(HTML_FOOTER)

def dfs2html(dfs, names, title='dataframe in html', filename="dataframe.html"):
    if len(dfs) != len(names):
        print 'dfs and names differ in length'
        return
    with open(filename, 'w') as html:
        html.write(HTML_HEADER)
        html.write(HTML_STYLE_HEADER)
        html.write(HTML_STYLE_FOOTER)
        html.write('\n<title>' + title + '</title>\n')
        for df, name in zip(dfs, names):
            html.write('\n<br/><b>' + name + '</b><br/><br/>\n')
            html.write(df.to_html())
        html.write(HTML_FOOTER)
