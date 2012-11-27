# -*- coding: utf-8 -*-
import re

#pylint: disable-msg=C0103
def re_filter(conf):
    return conf or r'[^/]+'

def path_filter(conf):
    return r'.+?'

def default_filter(conf):
    return r'[^/]+'
    
class Path(object):
    SYNTAX = re.compile(                                  \
        '(\\\\*)'                                         \
        '(?:(?::([a-zA-Z_][a-zA-Z_0-9]*)?()(?:#(.*?)#)?)' \
        '|(?:<([a-zA-Z_][a-zA-Z_0-9]*)?(?::([a-zA-Z_]*)'  \
        '(?::((?:\\\\.|[^\\\\>]+)+)?)?)?>))')

    #pylint: disable-msg=W0212
    def __init__(self, rule):
        self._filters = {
            're': re_filter,
            'path': path_filter,
            'default': default_filter
        }
        self._re  = None
        self._rule = rule
        self._pattern = [rule, rule]

        static, pattern = True, ''
        for key, mode, conf in self.__parse():
            if mode:
                static   = False
                mask     = self._filters[mode](conf)
                pattern += '(?P<%s>%s)' % (key, mask) if key else '(?:%s)' % mask
            elif key:
                pattern += re.escape(key)

        # route is a regex
        if not static:
            # store flat pattern
            func = lambda m: m.group(0) if len(m.group(1)) % 2 else m.group(1) + '(?:'
            self._pattern[1] = re.sub(r'(\\*)(\(\?P<[^>]*>|\((?!\?))', func, pattern) 
            try:
                self._pattern[0] = '^%s$' % pattern
                _ = re.compile(self._pattern[0]).match
            except re.error, ex:
                error = "Bad Route: %s (%s)" % (self._rule, ex.message)
                raise RouteSyntaxError(error)

    def __parse(self):
        """
        Parses a rule into a (name, filter, conf) token stream. If mode is
        None, name contains a static rule part.
        """
        offset, prefix = 0, ''
        for match in self.SYNTAX.finditer(self._rule):
            prefix += self._rule[offset:match.start()]
            grp = match.groups()
            # Escaped wildcard
            if len(grp[0])%2:
                prefix += match.group(0)[len(grp[0]):]
                offset  = match.end()
                continue
            # ??
            if prefix:
                yield prefix, None, None
            # ??
            name, filtr, conf = grp[1:4] if not grp[2] is None else grp[4:7]
            filtr = filtr or "default"
            yield name, filtr, conf or None
            offset, prefix = match.end(), ''
        # ??
        if offset <= len(self._rule) or prefix:
            yield prefix+self._rule[offset:], None, None

    def __repr__(self):
        return "<%s>" % self._pattern[0]

    def __eq__(self, other):
        return self.name == other.name
        
    @property
    def name(self):
        """Route name"""
        return self._pattern[1]

    @property
    def pattern(self):
        """Full pattern route"""
        return self._pattern[0]

    def match(self, path):
        """Get a collection of valid matches"""
        if self._re is None:
            self._re = re.compile(self._pattern[0])
        match = self._re.match(path)
        return None if match is None else match.groupdict()

