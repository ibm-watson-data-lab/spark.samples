# -------------------------------------------------------------------------------
# Copyright IBM Corp. 2016
# 
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -------------------------------------------------------------------------------

from ..display import Display
from pyspark.sql import DataFrame

class TableDisplay(Display):
    def doRender(self):
        if isinstance(self.entity, DataFrame):
            self.renderDataFrame()
            return
            
        self._addHTML("""
        <table class="table table-bordered table-condensed table-hover">
            <thead>
                <th>col1</th>
                <th>col2</th>
            </thead>
            <tbody>
                <tr>
                    <td>Row1 Col1 value</td>
                    <td>Row1 Col2 Value</td>
                </tr>
            </tbody>
        </table>
        """
        )
        
    def renderDataFrame(self):
        schema = self.entity.schema
        self._addHTML("""<table class="table table-bordered table-condensed table-hover"><thead>""")                   
        for field in schema.fields:
            self._addHTML("<th>" + field.name + "</th>")
        self._addHTML("</thead>")
        self._addHTML("<tbody>")
        for row in self.entity.take(100):
            self._addHTML("<tr>")
            for field in schema.fields:
                self._addHTML("<td>" + self._safeString(row[field.name]) + "</td>")
            self._addHTML("</tr>")
        self._addHTML("</tbody>")
        self._addHTML("</table>")
        
        
