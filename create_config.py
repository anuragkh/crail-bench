from __future__ import print_function
from six.moves import configparser
import sys

config = configparser.ConfigParser()
config.read('conf/crail-bench.conf')
print('Config: {}'.format(config))
config['crail']['size'] = sys.argv[1]
config['crail']['num_ops'] = sys.argv[2]
with open('conf/crail-bench.ini', 'w') as f:
  config.write(f)
