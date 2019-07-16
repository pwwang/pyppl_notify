import sys
from pathlib import Path
import yaml
from pyppl.plugin import hookimpl, pypplPreRunFunc
from pyppl.logger import logger
from pyppl.utils import fs, Box
from pyppl.exception import ProcAttributeError
from .flowchart import Flowchart

__version__ = "0.0.1"

@hookimpl
def setup(config):
	config['_flowchart'] = dict(theme = 'default')
	config['hide'] = False

def pyppl_allroutes(ppl):
	"""@API
	Show all the routes in the log.
	@returns:
		(PyPPL): The pipeline object itppl.
	"""
	logger.debug('ALL ROUTES:')
	#paths  = sorted([list(reversed(path)) for path in ppl.tree.getAllPaths()])
	paths  = sorted([pnode.name() for pnode in reversed(apath)]
		for apath in ppl.tree.getAllPaths(check_hide = False))
	paths2 = [] # processes merged from the same procset
	for apath in paths:
		prevset = None
		path2    = []
		for pnode in apath:
			if not '@' in pnode:
				path2.append(pnode)
			else:
				procset = pnode.split('@')[-1]
				if not prevset or prevset != procset:
					path2.append('[%s]' % procset)
					prevset = procset
				elif prevset == procset:
					continue
		if path2 not in paths2:
			paths2.append(path2)
		# see details for procset
		#if path != path2:
		#	logger.logger.info('[  DEBUG] * %s' % (' -> '.join(path)))

	for path2 in paths2:
		logger.debug('* %s', ' -> '.join(path2))

def pyppl_flowchart (ppl, fcfile = None, dotfile = None):
	"""@API
	Generate graph in dot language and visualize it.
	@params:
		dotfile (file): Where to same the dot graph.
			- Default: `None` (`path.splitext(sys.argv[0])[0] + ".pyppl.dot"`)
		fcfile (file):  The flowchart file.
			- Default: `None` (`path.splitext(sys.argv[0])[0] + ".pyppl.svg"`)
			- For example: run `python pipeline.py` will save it to `pipeline.pyppl.svg`
	@returns:
		(PyPPL): The pipeline object itself.
	"""
	pyppl_allroutes(ppl)
	fcfile  = fcfile or (Path('.') / Path(sys.argv[0]).stem).with_suffix(
		'%s.pyppl.svg' % ('.' + str(ppl.counter) if ppl.counter else ''))
	dotfile = dotfile if dotfile else Path(fcfile).with_suffix('.dot')
	fchart  = Flowchart(fcfile = fcfile, dotfile = dotfile)
	fchart.setTheme(ppl.config._flowchart.theme)

	for start in ppl.tree.getStarts():
		fchart.addNode(start, 'start')
	for end in ppl.tree.getEnds():
		fchart.addNode(end, 'end')
		for apath in ppl.tree.getPathsToStarts(end, check_hide = True):
			for i, pnode in enumerate(apath):
				if i == 0:
					fchart.addNode(pnode)
					fchart.addLink(pnode, end)
				else:
					fchart.addNode(pnode)
					fchart.addLink(pnode, apath[i-1])

	fchart.generate()
	logger.info ('Flowchart file saved to: %s', fchart.fcfile)
	logger.info ('DOT file saved to: %s', fchart.dotfile)

@hookimpl
def pypplRegisterFunc(ppl):
	pypplPreRunFunc(ppl, 'flowchart', pyppl_flowchart)
