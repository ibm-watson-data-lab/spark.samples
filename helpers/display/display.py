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

handlers=[]
def registerDisplayHandler(handlerMetadata):
    handlers.append(handlerMetadata)
def getSelectedHandler(handlerId, entity):
    if handlerId is not None:
        return handlers[handlerId]
    else:
        #get the first handler that can render this object
        for handler in handlers:
            menuInfos = handler.getMenuInfo(entity)
            if ( menuInfos is not None and len(menuInfos)>0 ):
                return handler
    #we didn't find any, return the first
    return handlers[0]

class DisplayHandlerMeta(object):
    __metaclass__ = ABCMeta
    @abstractmethod
    def getMenuInfo(self):
        pass
    @abstractmethod
    def newDisplayHandler(self,entity):
        pass
    
class Display(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, entity):
        self.entity=entity
        self.html=""
        self.scripts=list()
    
    def render(self):
        self.doRender()
        #generate final HTML
        ipythonDisplay(HTML(self._wrapBeforeHtml() + self.html + self._wrapAfterHtml()))
        self._addScriptElements()
         
    @abstractmethod
    def doRender(self):
        raise Exception("doRender method not implemented")
    
    #@abstractmethod
    #def canRender(self):
    #    pass
    
    def noChrome(self, flag):
        self.noChrome=flag
        
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
    
    def _wrapBeforeHtml(self):
        if ( self.noChrome ):
            return ""
        
        menuTree=dict()    
        for handler in handlers:
            for menuInfo in handler.getMenuInfo(self.entity):
                categoryId=menuInfo['categoryId']
                if categoryId is None:
                    raise Exception("Handler missing category id")
                elif not categoryId in menuTree:
                    menuTree[categoryId]=[menuInfo]
                else:
                    menuTree[categoryId].append(menuInfo) 
        
        html=""        
        for key, menuInfoList in menuTree.iteritems():
            if len(menuInfoList)==1:
                html+="""
                    <a class="btn btn-small display-type-button active" id="toto" title="{0}">
                        <i class="fa {1}"></i>
                    </a>
                """.format(menuInfoList[0]['title'], menuInfoList[0]['icon'])
            else:
                html+="""
                    <div class="btn-group btn-small" style="padding-left: 4px; padding-right: 4px;">
                        <a class="btn btn-small display-type-button" title="{0}">
                            <i class="{1}"></i>
                        </a>
                        <a class="btn btn-small dropdown-toggle" data-toggle="dropdown" style="padding-left: 6px; padding-right: 6px">
                            <b class="caret"></b>
                        </a>
                        <div class="dropdown-menu" role="menu">
                            <div class="row-fluid" style="width: 220px;">
                                <ul class="span6 multicol-menu">
                """.format(self.getCategoryTitle(key), self.getCategoryIconClass(key))
                for menuInfo in menuInfoList:                    
                    html+="""
                                    <li>
                                        <a href="#" class="display-type-button">
                                            <i class="fa {0}"></i>
                                            {1}
                                        </a>
                                    </li>
                    """.format(menuInfo['icon'],menuInfo['title'])
                html+="""
                                </ul>
                            </div>
                        </div>
                    </div>
                """
        return html
        
    def getCategoryTitle(self,catId):
        if catId == "Table":
            return "Table"
        elif catId == "Map":
            return "Map"
        else:
            return ""
            
    def getCategoryIconClass(self,catId):
        if catId == "Table":
            return "fa-table"
        elif catId == "Map":
            return "fa-map"
        else:
            return ""
        
    def _wrapAfterHtml(self):
        if ( self.noChrome ):
            return ""
        return ""
