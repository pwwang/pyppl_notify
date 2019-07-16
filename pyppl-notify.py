from email.mime.text import MIMEText
from datetime import datetime
from pyppl.plugin import hookimpl
from pyppl.logger import logger

__version__ = "0.0.1"

EMAIL = None
TEMPLATES = dict(
	ppl_begin = """Pipeline started

Time: {now}
Start processes: {obj.tree.starts}
""",

	ppl_end = """Pipeline finished

Time: {now}
Start processes: {obj.tree.starts}
Processes:
{obj.procs}
""",

	proc_begin = """Process {obj.id} started

Time: {now}
Size: {obj.size}
Pipeline directory: {obj.ppldir}
Process workdir: {obj.workdir}
""",

	proc_end = """Process {obj.id} finished

Time: {now}
Size: {obj.size}
Pipeline directory: {obj.ppldir}
Process workdir: {obj.workdir}
""",

	proc_abort = """Process {obj.id} failed

Time: {now}
Size: {obj.size}
Pipeline directory: {obj.ppldir}
Process workdir: {obj.workdir}
""",

	job_begin = """Process {obj.proc.id} #{obj.index} started

Time: {now}
Job directory: {obj.dir}
""",

	job_end = """Process {obj.proc.id} #{obj.index} finished

Time: {now}
Job directory: {obj.dir}
""",

	job_abort = """Process {obj.proc.id} #{obj.index} failed

Time: {now}
Job directory: {obj.dir}
""",
)

DEFAULT_CONFIG = {
	'from': 'pyppl-notify@no-reply.info',
	'to'  : [],
	'when': {
		'pipeline': 'abe',
		'proc': 'abe',
		'job': ''
	},
	'server'  : 'localhost',
	'ssl'     : False,
	'port'    : 25,
	'username': '',
	'password': ''
}

class Email:

	def __init__(self, config):
		self.config = config
		if config['ssl']:
			from smtplib import SMTP_SSL as SMTP
		else:
			from smtplib import SMTP
		self.smtp = SMTP(config['server'], int(config['port']))
		if config['username']:
			self.smtp.login(config['username'], config['password'])

	def send(self, objname, obj, status):
		now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		subject, text = TEMPLATES[objname + "_" + status].format(obj = obj, now = now).split('\n', 1)
		msg = MIMEText(text, "plain")
		msg["Subject"] = subject[8:].lstrip() if subject[:8] in ('Subject:', 'SUBJECT:') else subject
		msg["From"] = self.config['from']
		to = self.config['to']
		if not isinstance(to, list):
			to = [to]
		self.smtp.sendmail(self.config['from'], to, msg.as_string())

@hookimpl
def setup(config):
	conf = config.get('_notify', {})
	for key, val in DEFAULT_CONFIG.items():
		conf[key] = conf.get(key, val)
		if isinstance(val, dict):
			for k, v in val.items():
				conf[key][k] = conf[key].get(k, v)
	config['_notify'] = conf

@hookimpl
def pypplPreRun(ppl):
	"""A set of functions run when pipeline starts"""
	# initiate EMAIL
	global EMAIL
	if not EMAIL:
		EMAIL = Email(ppl.config._notify)
	if 'b' in ppl.config._notify.when['pipeline']:
		logger.debug('Notifying pipeline begins')
		EMAIL.send('ppl', ppl, 'begin')

@hookimpl
def pypplPostRun(ppl):
	"""A set of functions run when pipeline ends"""
	if 'e' in ppl.config._notify.when['pipeline']:
		logger.debug('Notifying pipeline ends')
		EMAIL.send('ppl', ppl, 'end')

@hookimpl
def procPreRun(proc):
	"""After a process starts"""
	if 'b' in proc.config._notify.when['pipeline']:
		logger.debug('Notifying process begins')
		EMAIL.send('proc', proc, 'begin')

@hookimpl
def procPostRun(proc):
	"""After a process has done"""
	if 'e' in proc.config._notify.when['pipeline']:
		logger.debug('Notifying process ends')
		EMAIL.send('proc', proc, 'end')

@hookimpl
def procFail(proc):
	"""When a process fails"""
	if 'a' in proc.config._notify.when['pipeline']:
		logger.debug('Notifying process fails')
		EMAIL.send('proc', proc, 'abort')

@hookimpl
def jobPreRun(job):
	"""A set of functions run when job starts"""
	if 'b' in job.proc.config._notify.when['pipeline']:
		logger.debug('Notifying job begins')
		EMAIL.send('job', job, 'begin')

@hookimpl
def jobPostRun(job):
	"""A set of functions run when job ends"""
	if 'e' in job.proc.config._notify.when['pipeline']:
		logger.debug('Notifying job ends')
		EMAIL.send('job', job, 'end')

@hookimpl
def jobFail(job):
	"""A set of function run when job fails"""
	if 'a' in job.proc.config._notify.when['pipeline']:
		logger.debug('Notifying job fails')
		EMAIL.send('job', job, 'abort')
