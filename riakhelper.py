#!/usr/bin/env python
""" Docstring """
from collections import OrderedDict
import json
import subprocess
import sys
from ConfigParser import SafeConfigParser, NoOptionError

class RiakHelper(object):
    """ Docstring """

    HEADER = '\n###############################################\n' + \
               '                 RIAK HELPER                   \n' + \
               '###############################################\n'

    PRETTY_PRINT_JSON_LIMIT = 5000
    PRETTY_PRINT_TERMS_LIMIT = 500

    CONFIG_FILENAME = 'riakhelper.cfg'
    CONFIG_SECTION = 'DEFAULT'
    CONFIG_HOST = 'host'
    CONFIG_PORT = 'port'
    CONFIG_DEFAULT_BUCKET = 'defaultBucket'

    def __init__(self):
        self._populateConfig()

        self.menu = OrderedDict()
        self.menu['1'] = ('Lookup a full record', self._lookup)
        self.menu['2'] = ('Lookup indexes on a record', self._lookupIndexes)
        self.menu['3'] = ('List available buckets', self._listBuckets)
        self.menu['4'] = ('List available keys in bucket', self._listKeys)
        self.menu['5'] = ('Perform secondary index query', self._indexQuery)
        self.menu['6'] = ('Perform secondary index query (Returning terms)', self._indexQueryReturnTerms)
        self.menu['7'] = ('Produce secondary index query with regex', self._produceIndexQueryRegex)
        self.menu['8'] = ('Show config', self._showConfig)
        self.menu['9'] = ('Exit', self._exit)

        self.baseUrl = 'http://{0}:{1}'.format(self.config[self.CONFIG_HOST], self.config[self.CONFIG_PORT])

    ############## MAIN FUNCTIONS ##############

    def run(self):
        """ Docstring """
        while True:
            self._outputMenu()

            try:
                option = raw_input('\nEnter option: ')
            except SyntaxError:
                self._continue()

            try:
                self.menu.get(option, ('', self._continue))[1]()
            except (NameError, IndexError, SyntaxError, TypeError):
                self._continue()

    def _populateConfig(self):
        """ Docstring """
        config = SafeConfigParser()
        config.read(self.CONFIG_FILENAME)
        try:
            self.config = {self.CONFIG_HOST : config.get(self.CONFIG_SECTION, self.CONFIG_HOST),
                           self.CONFIG_PORT : config.get(self.CONFIG_SECTION, self.CONFIG_PORT),
                           self.CONFIG_DEFAULT_BUCKET : config.get(self.CONFIG_SECTION, self.CONFIG_DEFAULT_BUCKET)}
        except NoOptionError:
            print "ERROR: Please ensure '{0}', '{1}' and '{2}' are set in '{3}'".format(self.CONFIG_HOST,
                                                                                        self.CONFIG_PORT,
                                                                                        self.CONFIG_DEFAULT_BUCKET,
                                                                                        self.CONFIG_FILENAME)
            sys.exit(1)

    def _lookup(self):
        """ Docstring """
        try:
            bucket = raw_input('\nEnter bucket (Leave blank for default = {0}): '\
                                                                .format(self.config[self.CONFIG_DEFAULT_BUCKET]))
            key = raw_input('Enter key: ')
        except SyntaxError:
            self._continue()
        self._query(key, bucket)

    def _lookupIndexes(self):
        """ Docstring """
        try:
            bucket = raw_input('\nEnter bucket (Leave blank for default = {0}): '\
                                                                .format(self.config[self.CONFIG_DEFAULT_BUCKET]))
            key = raw_input('Enter key: ')
        except SyntaxError:
            self._continue()
        self._queryIndexes(key, bucket)

    def _listBuckets(self):
        """ Docstring """
        command = 'curl -is {0}/buckets?buckets=true'.format(self.baseUrl)
        result = self._executeCommand(command)
        sortedResult = sorted(eval(result).get('buckets'))
        print sortedResult

    def _listKeys(self):
        """ Docstring """
        try:
            bucket = raw_input('\nEnter bucket: ')
        except SyntaxError:
            self._continue()

        command = 'curl -is {0}/buckets/{1}/keys?keys=true'.format(self.baseUrl, bucket)
        result = self._executeCommand(command)
        sortedResult = sorted(eval(result).get('keys'))
        print sortedResult

    def _indexQuery(self):
        """ Docstring """
        command = self._buildIndexQuery()
        result = self._executeCommand(command)
        sortedResult = sorted(eval(result).get('keys'))
        print sortedResult

    def _indexQueryReturnTerms(self):
        """ Docstring """
        command = self._buildIndexQuery(returnTerms=True)
        result = self._executeCommand(command)
        sortedResult = sorted(eval(result).get('results'))
        if len(sortedResult) < self.PRETTY_PRINT_TERMS_LIMIT:
            for line in sortedResult:
                print line
        else:
            print sortedResult

    def _produceIndexQueryRegex(self):
        """ Docstring """
        command = self._buildIndexQuery()

        try:
            regex = raw_input('Enter regex: ')
        except SyntaxError:
            self._continue()

        print "{0}?term_regex='{1}'".format(command, regex)

    def _showConfig(self):
        """ Docstring """
        print '\nCurrent config: {0}'.format(self.config)

    ############## HELPER FUNCTIONS ##############

    def _executeCommand(self, command):
        """ Docstring """
        print '\nExecuting command... \n\n> {0}'.format(command)
        try:
            input('\nHit ENTER to confirm or type anything to cancel\n')
        except SyntaxError:
            data = subprocess.check_output(command.split()).split('\n')
            indexes = False
            for line in data:
                if line.startswith('x-riak-index'):
                    indexes = True
                    print '{0}'.format(line)
            if indexes:
                print '\n'
            return data[-1]
        except NameError:
            self._continue()

    def _buildIndexQuery(self, returnTerms=False):
        """ Docstring """
        try:
            bucket = raw_input('\nEnter bucket: ')
            index = raw_input('Enter index: ')
            rangeStart = raw_input('Enter range start: ')
            rangeEnd = raw_input('Enter range end: ')
        except SyntaxError:
            self._continue()

        query = 'curl -is {0}/buckets/{1}/index/{2}/{3}/{4}'.format(self.baseUrl, bucket, index,
                                                                    rangeStart, rangeEnd)

        if returnTerms:
            query += '?return_terms=true'

        return query

    def _query(self, key, bucket=None):
        """ Docstring """
        if not bucket:
            bucket = self.config[self.CONFIG_DEFAULT_BUCKET]
        command = 'curl -is {0}/buckets/{1}/keys/{2}'.format(self.baseUrl, bucket, key)
        result = self._executeCommand(command)
        if 0 < len(result) < self.PRETTY_PRINT_JSON_LIMIT:   
	    jsonResult = json.loads(result)
            result = json.dumps(jsonResult, indent=3)
        print result

    def _queryIndexes(self, key, bucket=None):
        """ Docstring """
        if not bucket:
            bucket = self.config[self.CONFIG_DEFAULT_BUCKET]
        command = 'curl -is {0}/buckets/{1}/keys/{2}'.format(self.baseUrl, bucket, key)
        self._executeCommand(command)

    def _outputHeader(self):
        """ Docstring """
        print self.HEADER

    def _outputMenu(self):
        """ Docstring """
        self._outputHeader()
        for key, val in self.menu.iteritems():
            print '{0}) {1}'.format(key, val[0])

    def _exit(self):
        """ Docstring """
        sys.exit(0)

    def _continue(self):
        """ Docstring """
        pass

############## MAIN APPLICATION ##############

if __name__ == "__main__":
    helper = RiakHelper()
    helper.run()
