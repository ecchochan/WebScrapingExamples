#cython: boundscheck=False, wraparound=False, nonecheck=False, cdivision=True
#distutils: language = c++

'''

    Examples:
    
    # for item in `.left_col`:
    #     find `.cname` to yield "name"   : this.text.strip()
    #     find `.addr`  to yield "address": this.text.strip()
    
    schema = {
        '.left_col `item`':[{
            '.cname `name`': 'text',
            '.addr `address`': 'text'
        }]
    }

    parser = HTMLSchemaParser(schema)




    # for each `.etym` , *use its attribute `title` as the field name
    #     yield `title`    -> text
    
    '.etym `:this["title"]`':[{
        'h3 strong `title`': text,
    }]




    # for each `.gramb > li`:
    #     find `.ind`  to yield "key": this.text.strip()
    #     find `this.findNext("ul")`  to yield "key": this.text.strip()
    #     # used to find element from `each`

    '.gramb > li `items`': [
        {
            '.ind `key`': text,
            '=this.findNext("ul") `value`': sense
        }
    ],




    #python 3.6+ only

    '.crossReference `crossReference`': {
        'a `ref`': 'text',        # get `a` first
        'a'      : 'remove',      # then remove `a`
        '`note`' : 'text',        # get text for the whole `.crossReference`
    },



'''



import requests
from bs4 import BeautifulSoup
import dateparser
import re
import brotli
from urllib.parse import quote, urljoin

multipliers = {'K':1000, 'M':1000000, 'B':1000000000, 'k':1000, 'm':1000000, 'b':1000000000}

number_regex = re.compile(r'\d+(?:\,\d+)*(?:\.\d+){0,1}(?:\s*['+''.join(multipliers.keys())+']){0,1}')

def string_to_number(string,ty=int):
    r = number_regex.search(string)
    if not r:
        return None
    
    r = r.group().replace(',','').replace(' ','')
    
    string = string.replace(',','')
    
    last_char = string[-1]
    
    if last_char.isdigit(): # check if no suffix
        return ty(string)
    
    if last_char not in multipliers:
        print('!!! WARNING !!! multiplier not recognized! string:',repr(string))
        return None
    
    mult = multipliers[last_char] # look up suffix to get multiplier
     # convert number to float, multiply by multiplier, then make int
    return ty(float(string[:-1]) * mult)


import re
import math
chs_arabic_map = {
        u'零':0, u'一':1, u'二':2, u'三':3, u'四':4,'两':2, '兩':2,
        u'五':5, u'六':6, u'七':7, u'八':8, u'九':9,
        u'十':10, u'百':100, u'千':10 ** 3, u'万':10 ** 4,
        u'〇':0, u'壹':1, u'贰':2, u'叁':3, u'肆':4,
        u'伍':5, u'陆':6, u'柒':7, u'捌':8, u'玖':9,
        u'拾':10, u'佰':100, u'仟':10 ** 3, u'萬':10 ** 4,
        u'亿':10 ** 8, u'億':10 ** 8, u'幺': 1,
        u'０':0, u'１':1, u'２':2, u'３':3, u'４':4,
        u'５':5, u'６':6, u'７':7, u'８':8, u'９':9
}

num_chars = set(''.join(chs_arabic_map.keys()) + '點点')
zh_number_regex = re.compile('(['+''.join(num_chars) + ']+)')


def convertChineseDigitsToArabic (chinese_digits):
    chinese_digits = zh_number_regex.split(chinese_digits)
    return ''.join(_convertChineseDigitsToArabic(e) if e and e[0] in num_chars else e for e in chinese_digits)
    

def _convertChineseDigitsToArabic (chinese_digits):
    result  = 0
    tmp     = 0
    hnd_mln = 0
    last_digit = 0
    
    if chinese_digits == '两' or chinese_digits == '兩':
        return chinese_digits
    
    after = None
    decimals1 = chinese_digits.find('點')
    decimals2 = chinese_digits.find('点')
    if decimals1 == -1:
        decimals = decimals2
    elif decimals2 == -1:
        decimals = decimals1
    else:
        decimals = min(decimals1, decimals2)
    
    if decimals == -1:
        after = None
    else:
        after = chinese_digits[decimals+1:]
        chinese_digits = chinese_digits[:decimals]
    
    length = len(chinese_digits)
    last = length - 1
    
    for count in range(length):
        curr_char  = chinese_digits[count]
        
        if curr_char not in chs_arabic_map:
            return chinese_digits
        curr_digit = chs_arabic_map[curr_char]
        # meet 「亿」 or 「億」
        if curr_digit == 100000000:
            result  = result + tmp
            result  = int(result * curr_digit)
            # get result before 「亿」 and store it into hnd_mln
            # reset `result`
            hnd_mln = int(hnd_mln * 100000000 + result)
            result  = 0
            tmp     = 0
            last_digit = curr_digit
        # meet 「万」 or 「萬」
        elif curr_digit == 1000:
            result = result + tmp
            result = int(result * curr_digit)
            last_digit = curr_digit
            tmp    = 0
        # meet 「十」, 「百」, 「千」 or their traditional version
        elif curr_digit >= 10:
            tmp    = 1 if tmp == 0 else tmp
            result = int(result + curr_digit * tmp)
            tmp    = 0
            last_digit = curr_digit
        # meet single digit
        else:
            if count == last and (curr_char == '兩' or curr_char == '两'):
                break
            if curr_digit == 0:
                last_digit = 0
            tmp = int(tmp * 10 + curr_digit)
    if last_digit > 0:
        tmp = int(tmp * int(last_digit / 10))
        print(tmp)
        #if tmp > 0:
        #    tmp /= int(10 ** int(math.log10(tmp)))
    result = result + tmp
    result = result + hnd_mln
    
    _result = str(result)
    if after is not None:
        _result = _result + '.' + convertChineseDigitsToArabic(after)
    if (curr_char == '兩' or curr_char == '两'):
        _result += curr_char
        
    return _result

chin2num = convertChineseDigitsToArabic





import dateparser
import re
dateparser_parse = dateparser.parse

chin_numbers = re.compile('十{0,1}[一二三四五六七八九]|[一二三四五六七八九]十{0,1}|[零〇]|十')

def chin_num_normalize(text):
    return text.replace('卅','三十').replace('廿','二十')


def chin_num_sub(e):
    e = e.group()
    return str(chin2num(e))

def parse_datetime(text):
    dt = chin_numbers.sub(chin_num_sub, chin_num_normalize(text))
    return dateparser_parse(dt)


def _datetime(soup):
    if len(soup) > 0:
        return parse_datetime(soup[0].text.strip())
    else:
        return None


def timestamp(soup):
    if len(soup) > 0:
        parsed = parse_datetime(soup[0].text.strip())
        if not parsed:
            return None
        return parsed.timestamp()
    else:
        return None

def integer(soup):
    if len(soup) > 0:
        return string_to_number(soup[0].text.strip())
    else:
        return None
    
def _float(soup):
    if len(soup) > 0:
        return string_to_number(soup[0].text.strip(), float)
    else:
        return None

def text(soup):
    if len(soup) > 0:
        return soup[0].text.strip().replace('\xa0',' ')
    else:
        return None

def boolean(soup):
    if len(soup) > 0:
        return bool(soup[0].text.strip())
    else:
        return False
    
def remove(soup):
    for e in soup:
        e.extract()
    
def innerHTML(soup):
    if len(soup) > 0:
        soup[0].decode_contents().strip()
    else:
        return None
        
def outerHTML(soup):
    if len(soup) > 0:
        return str(soup[0]).strip()
    else:
        return None
    
from datetime import datetime
    
defaults = {
    int: integer,
    float: _float,
    str: text,
    bool: boolean,
    datetime: _datetime,
    'integer': integer,
    'float': _float,
    'text': text,
    'datetime': _datetime,
    'bool': boolean,
    'boolean': boolean,
    'timestamp': timestamp,
    'remove': remove,
    'innerHTML': innerHTML,
    'outerHTML': outerHTML,
    'html': innerHTML
}

headers = {}
headers["User-Agent"] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.25 Safari/537.36'
def fetch_url(url,retries=5, encoding='utf-8', reture_respone=False, **kwargs):
    _headers = headers
    if 'headers' in kwargs:
        _headers.update(kwargs['headers'])
        del kwargs['headers']
        
    for _ in range(retries+1):
        try:
            r = requests.get(url,headers=_headers, **kwargs)
            r.encoding = 'utf-8'
            break

        except Exception as e:
            if _ == retries:
                raise Exception('Retries over %s times'%retries)
            print('retrying... (',e,')')


    if 'Transfer-Encoding' in r.headers and r.headers['Transfer-Encoding'] == 'chunked':
        try:
            html = brotli.decompress(r.content).decode()
        except:
            html = r.text
    else:
        html = r.text
    if reture_respone:
        return html, r
    return html
        

class HTMLSchemaParser():
    def __init__(self,schema,ignore_important=True):
        self.schema = schema
        self.ignore_important = ignore_important

    def parse(self,soup):
        if isinstance(soup,str):
            soup = BeautifulSoup(soup, 'html.parser')
        return self._parse(soup,self.schema)

    def _parse(self,soup,schema,must=False,_last_key=None):
        result = None
        ignore_important = self.ignore_important
        if not isinstance(soup,list):
            soup = [soup]
        if isinstance(schema,dict):
            result = {}
            if len(soup) > 0:
                soup = soup[0]
                for key,val in schema.items():
                    _must = must
                    if '!important' in key:
                        _must = True
                        key = key.replace("!important", "")
                    name = key
                    if '`' in key:
                        key = key.split('`')
                        name = key[1]
                        key = key[0].strip()
                    
                    if len(key) == 0:
                        sub = self._parse(soup,val,_must,_last_key=_last_key)
                    else:
                        sub = self._parse(soup.select(key),val,must=_must,_last_key=key)
                    if sub is not None:
                        result[name] = sub
            elif must:
                raise Exception('key not found')
        elif isinstance(schema,list):
            if len(schema) > 0:
                schema = schema[0]
                result = []
                for each in soup:
                    try:
                        result.append(self._parse(each,schema,must=must,_last_key=_last_key))
                    except Exception as e:
                        if not ignore_important:
                            raise e

        elif isinstance(schema,str):
            if len(soup) > 0:
                this = soup[0]
                result = eval(schema)
                
        elif callable(schema):
            result = schema(soup)

        return result
            
class HTMLSchemaParser():
    def __init__(self,
                 schema,
                 ignore_important=False  # if set to False, raise Error if element with `!important` is not found
                ):
        self.schema = schema
        self.ignore_important = ignore_important

    def parse(self,
              soup,
              ignore_important=None,    # if set to None, this value follows the one from init
              force_sort = False,       # sort all array by its index in the whole html
              debug = False,
             ):
        if isinstance(soup,str):
            soup = BeautifulSoup(soup, 'html.parser')
        self.all_tags = soup.select('*')
        self.last_soup = soup
        return self._parse(soup,self.schema, ignore_important= ignore_important, force_sort = False, debug=debug)

    def _parse(self,soup,schema,must=False,_last_key=None,ignore_important=None, force_sort = False, debug=False):
        result = None
        ignore_important = self.ignore_important if ignore_important is None else ignore_important
        
        
        if not isinstance(soup,list):
            soup = [soup]
        if isinstance(schema,dict):
            result = {}
            if len(soup) > 0:
                soup = soup[0]
                for key,val in schema.items():
                    _must = must
                    if '!important' in key:
                        _must = True
                        key = key.replace("!important", "")
                    key = key.strip()
                    name = ''
                    if '`' in key:
                        key = key.split('`')
                        name = key[1]
                        key = key[0].strip()
                    
                    this = soup
                    if name.startswith('='):
                        name = eval(name[1:])
                    elif name.startswith(':'):
                        if isinstance(val,list):
                            _schema = val[0]
                            for each in (sorted(soup.select(key), key=lambda x:self.all_tags.index(x)) if force_sort else soup.select(key)):
                                try:
                                    this = each
                                    v = self._parse(each,_schema,must=must,_last_key=_last_key, force_sort = force_sort)
                                    result[eval(name[1:])] = v
                                except Exception as e:
                                    if not ignore_important:
                                        raise e
                            continue
                        else:
                            name = eval(name[1:])
                        
                    
                    if len(key) == 0:
                        sub = self._parse(soup,val,_must,_last_key=_last_key, force_sort = force_sort)
                    else:
                        
                        
                        if key.startswith('='):
                            this = soup
                            sub = self._parse(eval(key[1:]),val,must=_must,_last_key=key, force_sort = force_sort)
                        elif key == 'index':
                            result['index'] = self.all_tags.index(soup)
                        else:
                            sub = self._parse((sorted(soup.select(key), key=lambda x:self.all_tags.index(x)) if force_sort else soup.select(key)),val,must=_must,_last_key=key, force_sort = force_sort)
                    if sub and len(name) > 0:
                        if isinstance(sub,dict) and len(sub) == 1 and list(sub.keys())[0] == name:
                            sub = sub[name]
                        result[name] = sub
                        
                        
                        
                        
            elif must:
                print(self.last_soup.prettify())
                raise Exception('key not found')
        elif isinstance(schema,list):
            if len(schema) > 0:
                schema = schema[0]
                result = []
                for each in soup:
                    try:
                        a = self._parse(each,schema,must=must,_last_key=_last_key, force_sort = force_sort)
                        if a:
                            result.append(a)
                    except Exception as e:
                        print(e)
                        if not ignore_important:
                            raise e

        elif isinstance(schema,str):
            if len(soup) > 0:
                if schema not in defaults:
                    this = soup[0]
                    if schema.startswith('href'):
                        prefix = ':'.join(schema.split(':')[1:])
                        result = this["href"]
                        if len(prefix) > 0 and len(result) > 0:
                            prefix = prefix.strip()
                            result = urljoin(prefix, result)
                            
                    else:
                        result = eval(schema)
                else:
                    result = defaults[schema](soup)
                
        elif callable(schema):
            result = schema(soup)

        return result
            
     