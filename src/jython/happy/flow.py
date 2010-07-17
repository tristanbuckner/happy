#========================================================================
# Copyright (c) 2008, Metaweb Technologies, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY METAWEB TECHNOLOGIES ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULARmore flo    
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL METAWEB TECHNOLOGIES BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ========================================================================

# following are for normal operation in happy
import happy
from happy.log import logger, setLevel
import happy.dfs as dfs
import happy.json as json
 
# following are only for unitest in pydev: cannot call happy
#import logging as logger
#logger.basicConfig(level=logger.INFO)

# following are required in both
from sets import Set as set
import copy
from types import ListType

def isIterable(obj):
    """
    @return True if the passed object is an iterable.
    """
    return isinstance(obj, ListType)

def castList(obj):
    """
    @return: if this object is already a list, the original list;
             otherwise, the object contained in a new list.
             (If None, return the empty list)
    """
    if (obj==None):
        return []
    else:
        return isIterable(obj) and obj or [obj]

def uniq(obj):
    """
    @return: if this object is already a list, the unique items in the original list;
             otherwise, the object contained in a new list.
    """
    o = castList(obj)
    s = set(o)
    l = list(s)
    return l

class FlowException(Exception):
    """ General-purpose exception for DAG & Flow issues."""
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr("%s" % self.value)    

class CycleException(FlowException):
    """
    Thrown if a cycle is detected in the graph,
    or if an action would create a cycle.
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr("Cycle in graph: %s" % self.value)    

class DAG(object):
    """
    A class for Directed Acyclic Graphs, with bidirectional 
    parent/child links. 
    
    An instance of this class acts both as a node in the DAG, 
    as well as a handle to the entire DAG -- defined as all 
    nodes connected to the handle by a parent/child relationship.
    The nodes(), sinks(), sources(), sort() and dictize() methods
    all work on the entire connected DAG.
    
    The class is thread-safe; multiple non-intersecting DAGs can
    be created at the same time.  Any node handle will only refer 
    to the graph it is in -- ie, its own locally connected nodes.
    
    The dependency chain of a Flow is implemented as a DAG.  The DAG
    class deals with the topology of the graph; its subclass 
    HappyJobNode handles running jobs and montioring status.  
    """
    
    def _nextId(cls, id=None):
        """
        Global classmethod to get the next ID and increment
        the internal counter.
        
        @param id: if passed, resets the global counter to that value
        """
        if (not hasattr(DAG, "_lastID")):
            DAG._lastID = 0
        if (id):
            DAG._lastId = id
        DAG._lastID = DAG._lastID + 1
        return DAG._lastID
    _nextId = classmethod(_nextId)

    def __init__(self, **kwargs):
        """
        Create a new DAG node.
        
        @param: name -- the name of the node
        @param: parents -- parents of this node
        @param: children -- children of this node
        @param: _id -- the id of the node (for deserialization only)
        """
        logger.debug("Creating DAG: %s" % kwargs)
        self._id = kwargs.get("_id", DAG._nextId())
        self.name = kwargs.get("name", None)
        self._parents = []
        self._children = []
        if (kwargs.has_key("parents")):
            self.addParent(kwargs["parents"])
        if (kwargs.has_key("children")):
            self.addChild(kwargs["children"])
        if not self.name:    
            self.name = self._auto_name()

    def __repr__(self):
        """ String representation for this object. """
        return '<%s "%s" (%s)>' % (self.__class__.__name__, self.name, self._id)

    def _kwargs(self):
        """
        Returns the kwargs required to re-create this node.
        Used by dictize() and dedictize().
        Subclasses should add their properties to this dict.
        
        @return: a dict of kwargs that can be used in __init__
        """
        dict = {"name":self.name}
        return dict

    def _auto_name(self):
        """
        Creates a unique name for this node, if one was not provided.
        """
        return "node_"+str(self._id)

    def parents(self):
        """ 
        Read-only property: return this node's parents.
        Use addParent() to modify the parent/child list.
        """
        return self._parents

    def children(self):
        """ 
        Read-only property: return this node's children.
        Use addChild() to modify the parent/child list.
        """
        return self._children

    def addChild(self, childNode):
        """
        Add a child (or list of children) to this node, 
        maintaining bidirectional links.
        
        Re-adding an already existing child is silently ignored.
        
        @param: childNode -- a DAG (or list of DAGs) to add as children.
        
        @raise CycleException if adding this link would create a cycle.
        """
        if (type(childNode)==list or isIterable(childNode)):
            for c in childNode:
                if (self.isDecendentOf(c)):
                    raise CycleException, "%s is a ancestor of %s, cannot add as child." % (childNode, self)
                if (c not in self.children()):
                    c._parents.append(self)
                    self._children.append(c)
        else:
            if (self.isDecendentOf(childNode)):
                raise CycleException, "%s is a ancestor of %s, cannot add as child." % (childNode, self)
            if (childNode not in self.children()):
                childNode._parents.append(self)
                self._children.append(childNode)

    def addParent(self, parentNode):
        """
        Add a parent (or list of parents) to this node,
        maintaining bidirectional links

        Re-adding an already existing parent is silently ignored.
        
        @param: parentNode -- a DAG (or list of DAGs) to add as parent(s).

        @raise CycleException if adding this link would create a cycle.
        """
        if (type(parentNode)==list or isIterable(parentNode)):
            for p in parentNode:
                if (self.isAncestorOf(p)):
                    raise CycleException, "%s is a decendent of %s, cannot add as parent." % (parentNode, self)
                if (p not in self.parents()):
                    p._children.append(self)
                    self._parents.append(p)
        else:
            if (self.isAncestorOf(parentNode)):
                raise CycleException, "%s is a decendent of %s, cannot add as parent." % (parentNode, self)
            if (parentNode not in self.parents()):
                parentNode._children.append(self)
                self._parents.append(parentNode)

    def isAncestorOf(self, node):
        """
        @return True if the passed node is a parent (grandparent, etc)
                of this node in the DAG
        """
        if (self in node.parents()):
            return True
        elif (not node.isSource()):
            return reduce(lambda x,y: x or y, [self.isAncestorOf(x) for x in node.parents()])
        else:
            return False

    def isDecendentOf(self, node):
        """
        @return True if the passed node is a child (grandchild, etc)
                of this node in the DAG
        """
        if (self in node.children()):
            return True
        elif (not node.isSink()):
            return reduce(lambda x,y: x or y, [self.isDecendentOf(x) for x in node.children()])
        else:
            return False

    def isSink(self):
        """
        @return True if this node is a sink -- ie, has no children
        """
        return (len(self.children()) == 0)

    def isSource(self):
        """
        @return True if this node is a source -- ie, has no parents
        """
        return (len(self.parents()) == 0)

    def nodes(self, visited=None):
        """
        Returns the set of all nodes that are in the DAG that contains
        this node.
        
        @param visited: The set of all nodes that have already been visited when
                        getting the set.  Used only by recursive calls, never by
                        initial call.
        """
        if (not visited):
            visited = set()
        if (self not in visited):
            visited.update([self])
            for node in self.parents():
                visited.update(node.nodes(visited=visited))
            for node in self.children():
                visited.update(node.nodes(visited=visited))
        return visited

    def sinks(self):
        """
        Find the leaf node(s) of the DAG that this node is in.
        
        @return a list containing all the parent nodes
        """
        s = [node for node in self.nodes() if node.isSink()]
        s.sort(lambda x,y: cmp(x._id, y._id))
        return s

    def sources(self):
        """
        Find the root node(s) of the DAG that this node is in.
        
        @return a list containing all the parent nodes
        """
        s = [node for node in self.nodes() if node.isSource()]
        s.sort(lambda x,y: cmp(x._id, y._id))
        return s

    def sort(self):
        """
        Return all of the nodes in topologically sorted order.
        Current implementation is inefficient; requires two
        passes -- one to find all sources, another to sort.
        """
        srt = self.sources()
        stack = list(srt)      # makes a copy
        while stack:
            node = stack.pop(0)
            if (not node.isSink()):
                # if a child is not in srt, and all of its parents are in srt,
                # then add it.  Must have all parents to get true topo sort.
                newChildren = filter(lambda x: len(set(x.parents()) - set(srt))==0,
                                     [child for child in node.children() if child not in srt])
                stack.extend(newChildren)
                srt.extend(newChildren)
        return srt

    def dictize(self):
        """
        Create a dictionary representation of this DAG.  Requires subclasses to 
        """
        dict = {}
        for node in self.sort():
            logger.debug("Dictize: id %s has name %s" % (node._id, node.name))
            x = node._kwargs()
            dict[node._id]={"klass":node.__class__.__name__, 
                            "kwargs": x,
                            "children":[child._id for child in node.children()]}
        return dict
    
    def dedictize(cls, dict):
        curId = DAG._nextId()
        lookup = {}
        maxId = -1
        rootNode = None
        ids = dict.keys()
        ids.sort()
        for id in ids:
            kwargs = dict[id]["kwargs"]
            thisId = curId+id
            kwargs["_id"] = thisId
            className = dict[id].get("klass", cls.__name__)
            classStr = "%s(**%s)" % (className, kwargs)
            node = eval(classStr)
            lookup[id] = node
            if (id>maxId): maxId=id
            if (not rootNode): rootNode=node
        for id in ids:
            lookup[id].addChild([lookup[child] 
                                 for child in dict[id]["children"]])
        DAG._nextId(maxId)
        return rootNode
    dedictize=classmethod(dedictize)


class NullJob(object):
    """
    A null job acts like a HappyJob, but doesn't do
    anything (except create an empty file) when run.  
    Used for testing Flows.
    """
    def __init__(self, **kwargs):
        self.name = kwargs.get("name", None)
        self.inputpaths = []
        self.outputpath = None
    
    def run(self):
        logger.info("NullJob %s fired." % self.name)
        w = dfs.write(self.outputpath)
        w.write("NullJob() output -- for testing only.")
        w.close()


class HappyJobNode(DAG):

    def __init__(self, **kwargs):
        logger.debug("Creating HappyJobNode.")
        jobparam = kwargs.get("job", None)
        if (not jobparam):
            self.job = NullJob()
        elif (type(jobparam)==str):
            self.job = eval(jobparam)
        else:
            self.job = jobparam
        DAG.__init__(self, **kwargs)
        self.force = kwargs.get("force", False)
        if kwargs.has_key("inputpaths"):
            self.job.inputpaths = kwargs.get("inputpaths")
        if kwargs.has_key("outputpath"):
            if (kwargs.get("outputpath") and not type(kwargs.get("outputpath"))==str):
                raise ValueError, "HappyJobNode.outputpath only accepts a single file; got %s." % kwargs.get("outputpath")
            self.job.outputpath = kwargs.get("outputpath")
        self.status = None

    def _kwargs(self):
        """
        Returns the kwargs required to re-create a HappyJobNode, 
        in conjunction with DAG.kwargs().
        Used by dictize() and dedictize().
        Subclasses should add their properties to this dict.
        
        @return: a dict of kwargs that can be used in __init__
        """
        dict = DAG._kwargs(self) 
        if (self.job):        
            dict["inputpaths"] = self.job.inputpaths
            dict["outputpath"] = self.job.outputpath
            dict["job"] = "%s()" % self.job.__class__.__name__
        return dict

    # wrapper properties around HappyJob.inputpaths
    def _setinputpaths(self, inputpaths):
        self.job.inputpaths = uniq(inputpaths)
    def _getinputpaths(self):
        return self.job.inputpaths
    inputpaths = property(fget=_getinputpaths, fset=_setinputpaths)

    # wrapper properties around HappyJob.outputpath
    def _setoutputpath(self, outputpath):
        self.job.outputpath = outputpath
    def _getoutputpath(self):
        return self.job.outputpath
    outputpath = property(fget=_getoutputpath, fset=_setoutputpath)
    
    # wrapper properties around HappyJob.name
    def _setname(self, name):
        self.job.name = name
    def _getname(self):
        return self.job.name
    name = property(fget=_getname, fset=_setname)

    def run(self, force=False, workingDir=None):
        """
        Runs the entire job chain (ie DAG) that contains this node.
        """
        logger.debug("Calling HappyJobNode.run(), workingDir=%s" % workingDir)
        self.linkNodes(workingDir)
        if force:
            self.deleteOutFiles(onlytmp=False)
        # stack = self.sources()
        stack = self.sort()
        logger.info("Stack order is: %s" % (", ".join([str(x._id) for x in stack],)))
        ok_children = self.sources()
        while stack:
            node = stack.pop(0)
            putChildren = False
            
            if (not node in ok_children):
                logger.warn("Branch terminated: node %s not in ok_children list %s." % (node, ok_children))
                continue
            
            pre = node.precheck()
            if node.force:
                logger.info("FORCING %s [%s --> %s] (delete %s first)" % (node, node.inputpaths, node.outputpath, node.outputpath))
                dfs.delete(node.outputpath)
                node.fire()
            elif (pre =='ready'):
                logger.info("Running %s [%s --> %s]" % (node, node.inputpaths, node.outputpath))
                node.fire()
            else:
                logger.info("Skipping job %s: already done" % node)
                putChildren = True
                self.status = 'skip'
            
            post = node.postcheck()    
            if (post == 'done'):
                logger.info("Job %s completed successfully. " % node)
                putChildren = True
            elif (post == 'fail'):
                logger.info("Job %s failed.  Not adding children." % node)

            if putChildren:
                if (node.isSink()):
                    logger.info("Job %s is a sink, no children." % node)
                else:
                    newChildren = [child for child in node.children() if child not in ok_children]
                    logger.info("Placing children %s of job %s on stack." %  (newChildren, node))
                    ok_children.extend(newChildren)

    def fire(self):
        """
        Runs this node's HappyJob.  Blocks until completed.
        """
        if (self.job):
            job = self.job
            try:
                job.run()
                logger.debug("Job run.  Setting status to done.")
                self.status = 'done'
            except Exception:
                logger.error("Caught exception.  Setting status to fail and deleting output.")
                dfs.delete(self.outputpath)
                self.status = 'fail'

    def linkNodes(self, workingDir=None):
        """
        Assures that every parent/child pair have a matching file in
        their inFile / outFile lists.  Creates files if necessary. 
        
        @param workingDir: the directory to create temp files in. 
        """
        if workingDir:
            logger.info("Linking nodes, using workingDir = %s" % (workingDir))    
            if dfs.exists(workingDir):
                fs = dfs.fileStatus(workingDir)
                if not fs.isDir():
                    raise FlowException, "%s is a file, not a directory." % (workingDir)
            else:
                logger.info("Creating working directory %s." % (workingDir))    
                # dfs.mkdir(workingDir)
        stack = self.sources()
        for source in stack:
            if ((not source.inputpaths) or len(source.inputpaths)<1):
                raise FlowException, "Source node %s has no inputpaths defined." % source
        while stack:
            node = stack.pop(0)
            if node.outputpath:
                logger.trace("linkNodes(): %s has an outputpath '%s'.  Using it." % (node, node.outputpath))
                filename = node.outputpath
            else:
                filename = "tmp.%s" % (node.name)
                if workingDir:
                    filename = "%s/%s" % (workingDir, filename)
                logger.trace("linkNodes(): Created temp outfile '%s' for %s." % (filename, node))
                node.outputpath = filename
            for child in node.children():
                if ((not child.inputpaths) or 
                   (len(set(node.outputpath) & set(child.inputpaths)) == 0)):
                    logger.debug("linkNodes(): Linked %s and %s with file '%s'." % (node, child, filename))
                    child.inputpaths = castList(child.inputpaths) + [filename]
                stack.append(child)
            logger.debug("%s has inputs %s and outputs %s" % (node, node.inputpaths, node.outputpath))

    def deleteOutFiles(self, onlytmp=True):
        """
        Deletes all files listed as outputs in the Flow.
        """
        self.linkNodes()
        for node in self.sort():
            file = node.outputpath
            if (not onlytmp or file[0:4]=='tmp.'):
                logger.info("Deleting output file '%s'" % file)
                dfs.delete(file)
    
    def precheck(self):
        """
        Checks to see if the preconditions for this HappyJob have been met.
        If so, returns true, and the HappyJob is executed.
        
        It is expected that this method will be overidden to implement custom
        checks in a subclass (use lamdbas instead?)
        
        @return: STATUS_READY if the HappyJob's preconditions are met and the job can be run.
                 STATUS_WAIT if the job is not ready to be run
                 STATUS_SKIP if the job has already been run
                 STATUS_ERROR if we should abort
        """
        if (not dfs.exists(self.outputpath)):
            logger.debug("precheck(%s): outputpath %s does not exist, ready to run." 
                         % (self, self.outputpath))
            return 'ready'
        inTSs = [dfs.modtime(file) for file in self.inputpaths]
        outTS = dfs.modtime(self.outputpath)
        newer = reduce(lambda x,y: x or y, [(inTS>outTS) for inTS in inTSs])
        logger.debug("Input timestamps: %s" % inTSs)
        logger.debug("Output timestamp: %s" % outTS)
        if newer:
            logger.debug("At least one input file is newer than outputfile, ready to run.")
            dfs.delete(self.outputpath)
            return 'ready'
        else:
            logger.debug("All input files are newer than outputfile, skipping.")
            return 'skip'
    
    def postcheck(self):
        """
        Checks to see if the postconditions for this HappyJob have been met.
        If so, returns true, and this Node's children are fired.
        
        It is expected that this method will be overidden to implement custom
        checks in a subclass (use lamdbas instead?)
        
        The default implementation returns the status set by fire():
        'ready' if the job completed, 'fail' is the job threw an exception
        
        @return: True if this HappyJob's postonditions are met. Children exec.
                 False if the children jobs should not be fired.
        """
        logger.debug("Postcheck status is %s" % self.status)
        return self.status

from triplequery import TripleQuery
class TripleQueryNode(HappyJobNode):
    """
    A TripleQueryNode handles the slightly different launching requirements 
    for a TripleQuery job. 
    """ 
    def __init__(self, **kwargs):
        logger.debug("Creating TripleQueryNode.")
        DAG.__init__(self, **kwargs)
        self.inputpaths = kwargs.get("inputpaths", None)
        self.outputpath = kwargs.get("outputpath", None)
        self.force = kwargs.get("force", False)
        self.query = kwargs.get("query", None)
        self.status = None

    name = None
    inputpaths = None
    outputpath = None

    def fire(self):
        """
        Runs this node's TripleQuery job.  Blocks until completed.
        """
        job = TripleQuery(self.query, self.inputpaths, self.outputpath)
        try:
            job.run()
            logger.debug("TripleQuery run.  Setting status to done.")
            self.status = 'done'
        except Exception:
            logger.error("Caught exception in TripleQuery.  Setting status to fail and deleting output.")
            dfs.delete(self.outputpath)
            self.status = 'fail'

    def _kwargs(self):
        """
        Returns the kwargs required to re-create a TripleQueryNode, 
        in conjunction with DAG.kwargs().
        Used by dictize() and dedictize().
        Subclasses should add their properties to this dict.
        
        @return: a dict of kwargs that can be used in __init__
        """
        dict = DAG._kwargs(self) 
        dict["inputpaths"] = self.inputpaths
        dict["outputpath"] = self.outputpath
        dict["query"] = self.query
        return dict


class NullNode(HappyJobNode):
    """
    A NullNode looks like a HappyJobNode, but it does nothing.
    Its outputpath gets set automatically to its inputpath,
    assuring no action accurs.  Used for splits / joins / connects.
    """ 
    def __init__(self, **kwargs):        
        logger.debug("Creating NullNode.")
        self._inputpaths = []
        self._outputpath = None
        self._parents = []
        self._children = []
        self.job = NullJob
        HappyJobNode.__init__(self, **kwargs)
        self._name = kwargs.get("name", None)
        self.force = kwargs.get("force", False)
        if kwargs.has_key("inputpaths"):
            self._inputpaths = kwargs.get("inputpaths")
            logger.debug("Using inputpaths=%s" % self._inputpaths)
            if len(self.job.inputpaths) > 1:
                raise ValueException, "NullNodes can only handle a single inputpath."
            self._outputpath = self.inputpaths[0]
        self.status = None

    def fire(self, *args, **kwargs):
        logger.info("NullNode %s fired." % self.name)
        
    # force inputpaths[0] == outputpath
    def _setinputpaths(self, inputpaths):
        logger.debug("Called NullNode._setinputpaths(%s)" % inputpaths)
        self._inputpaths = uniq(inputpaths)
        self._outputpath = inputpaths[0]
    def _getinputpaths(self):
        logger.debug("Called NullNode._getinputpaths() = %s" % self._inputpaths)
        return self._inputpaths
    inputpaths = property(fget=_getinputpaths, fset=_setinputpaths)

    # and inverse ... force inputpaths == [outputpath]
    def _setoutputpath(self, outputpath):
        logger.debug("Called NullNode._setoutputpath(%s)" % outputpath)
        self._inputpaths = [outputpath]
        self._outputpath = outputpath
    def _getoutputpath(self):
        logger.debug("Called NullNode._getoutputpath() = %s" % self._outputpath)
        return self._outputpath
    outputpath = property(fget=_getoutputpath, fset=_setoutputpath)

    # wrapper properties around HappyJob.name
    def _setname(self, name):
        self._name = name
    def _getname(self):
        return self._name
    name = property(fget=_getname, fset=_setname)
    


class Flow(object):
    """
    A flow class is a convenience wrapper for building DAGs of HappyJobNodes
    """
    def __init__(self, job=NullNode(name="root"), workingDir="tmp", cleanupTemp=False, inputpaths=None, outputpath=None, **kwargs):
        logger.debug("Creating Flow() object, workingDir=%s" % workingDir)
        self.startNode = job
        self.lastNode = job
        self.workingDir = workingDir
        self.cleanupTemp = cleanupTemp
        self.default_inputpaths = inputpaths
        self.default_outputpath = outputpath

    def __copy__(self):
        """
        Jython copy.copy() does not work by default.
        """
        logger.debug("Copying Flow() object.")
        c = Flow()
        c.workingDir = self.workingDir 
        c.cleanupTemp = self.cleanupTemp
        c.default_inputpaths = self.default_inputpaths
        c.default_outputpath = self.default_outputpath
        c.startNode = self.startNode
        c.lastNode = self.lastNode
        return c

    def chain(self, node=None, join=None):
        """
        Add a new node to the chain at lastNode return the modified object
        """
        if not node: node = NullJob()
        
        if join:
            logger.debug("In Flow.chain(): join=%s" % join)
            if (type(join) != type(list())): 
                join = [join]
            for jn in join:
                logger.debug("Joining node %s into chain %s." % (jn, node))
                jn.lastNode.addChild(node)

        logger.debug("Chaining %s to %s." % (self, node))
        self.lastNode.addChild(node)
        self.lastNode = node
        return self

    def _(self, job, name=None, node=HappyJobNode, join=None, force=False,
          inputpaths=None, outputpath=None):
        """
        Convenience shorthand for chaining functions without creating nodes.
        """
        if not name:
            name = self._auto_name(job)
        newnode = node(name=name, job=job, force=force, inputpaths=inputpaths, outputpath=outputpath)
        self.chain(node=newnode, join=join)

    def _auto_name(self, job):
        """
        Generates a unique name for this node, if one was not provided.
        """
        root = job.__class__.__name__
        nodes = list(self.lastNode.nodes())
        matches = [node.name for node in nodes if node.name.startswith(root)]
        logger.debug("Node names: %s" % nodes)
        if (len(matches)==0):
            return root + '_1'
        try:
            iter_str = [name.split('_')[-1] for name in matches]
            logger.debug("Node iter_str: %s" % iter_str)
            iters = [int(i) for i in iter_str]
            logger.debug("Node iters: %s" % iter_str)
            max_iter = max(iters) + 1
            logger.debug("max_iter: %s" % max_iter)
            return root + '_' + str(max_iter)
        except:
            logger.warn("Could not determine iteration: %s " % matches)
            return root + '_1'

    def force(self, force=True):
        """
        Forces the node that was defined immediately before this call
        be be re-executed.
        """
        if self.lastNode:
            self.lastNode.force = force

    def split(self, n=2):
        """
        Splits the current flow into multiple flows 
        @type n: int
        @param n: the number of flows to split into, must be >= 1
          
        @rtype: tuple
        @return: a tuple withn 'n' copies of the current flow
        
        @raise: ValueError if n is < 1
        """
        if n < 1: raise ValueError("Cannot split into less than 1 flow")
        if n == 1: return (self, )

        splits = list()
        for i in range(n):
            splits.append(copy.copy(self))

        return tuple(splits)

    def run(self):
        """
        Runs the flow.
        """
        logger.debug("Preparing to run Flow.")
        sources = self.startNode.sources()
        logger.debug("Sources: %s" % sources)
        for node in sources:
            if not node.inputpaths:
                logger.debug("Source %s does not have inputpaths, setting to: %s" % (node, self.default_inputpaths))
                node.inputpaths = self.default_inputpaths
        sinks = self.startNode.sinks()
        logger.debug("Sinks: %s" % sinks)
        for node in sinks:
            if not node.outputpath:
                logger.debug("Source %s does not have outputpath, setting to: %s" % (node, self.default_outputpath))
                node.outputpath = self.default_outputpath
        
        logger.debug("Calling HappyJobNode.run(), workingDir = %s" % self.workingDir)                
        self.startNode.run(force=False, workingDir=self.workingDir)

