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
from abc import ABCMeta,abstractmethod
from IPython.display import display as ipythonDisplay, HTML, Javascript
import sys

class Display(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, entity):
        self.entity=entity
        self.html=""
        self.scripts=list()
    
    def render(self):
        self.doRender()
        ipythonDisplay(HTML(self.html))
        self._addScriptElements()
         
    @abstractmethod
    def doRender(self):
        raise Exception("doRender method not implemented")
        
    def _addHTML(self, fragment):
        self.html+=fragment
        
    def _safeString(self, s):
        if not isinstance(s, str if sys.version >= '3' else basestring):
            return str(s)
        else:
            return s.encode('ascii', 'ignore')
        
    #def _addD3Script2(self):
    #    print("Adding d3")
    #    self._addScriptElement("//cdnjs.cloudflare.com/ajax/libs/d3/3.4.8/d3.min")
        
    def _addScriptElement(self, script):
        self.scripts.append(script)
        
    def _addScriptElements(self):
        code="(function(){var g,s=document.getElementsByTagName('script')[0];"
        for script in self.scripts:
            code+="""
            g=document.createElement('script');
            g.type='text/javascript';
            g.defer=false; 
            g.async=false; 
            g.src='{0}';
            s=s.parentNode.insertBefore(g,s).nextSibling;
            """.format(script)
        code+="})();"
        ipythonDisplay(Javascript(code))

        
    