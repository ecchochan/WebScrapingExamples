
import os
import glob
import json
from pprint import pprint
import os
import random
import sqlite3
from threading import Thread, Lock
from datetime import datetime
from time import sleep
def get_sqlite3_type(x):
    if isinstance(x,(int,bool)):
        return 'INTEGER'
    elif isinstance(x,float):
        return 'REAL'
    elif isinstance(x,bytes):
        return 'BLOB'
    return 'TEXT'


def dict_factory(cursor, row):
    return {col[0]:row[idx] for idx, col in enumerate(cursor.description)}


class JSONSQLite():
    
    def get_db(self):
        name = self.name
        self.db = db = sqlite3.connect('%s.sqlite'%name, 
                                       check_same_thread =False,timeout=10)
        return db
    
    def __init__(self, 
                 name, 
                 tables,
                 ignore_invalid = False,
                 foreign={},
                 drop=False):
        self.name = name
        db = self.get_db()
        db.isolation_level = None
        
        self.update_lock = Lock()
        self.new_data = new_data = {}
        self.new_data_ign = new_data_ign = {}
        self.tables = tables
        self.foreign = foreign
        self.ignore_invalid = ignore_invalid
    

        c = db.cursor()
        try:
            
            while True:
                try:
                    c.execute('BEGIN')
                    break
                except:
                    sleep(0.2)

            for table,vals in list(tables.items()):
                if isinstance(vals,str):
                    continue

                example = vals['structure']
                keys = vals['keys'] if 'keys' in vals else []
                indexes = vals['index'] if 'index' in vals else []
                if not isinstance(keys,(list,tuple,set)):
                    keys = [keys]
                if not isinstance(indexes,(list,tuple,set)):
                    indexes = [indexes]

                keys = set(keys)
                def _field(k,v,p=False):
                    return str(k) + ' ' + (v[1:] if isinstance(v,str) and v.startswith('=') else get_sqlite3_type(v) )

                if drop:
                    c.execute('DROP TABLE IF EXISTS %s'%table)
                sql = 'CREATE TABLE IF NOT EXISTS %s(%s)'%(table, ', '.join(  _field(k,v,k in keys) for k,v in example.items() if k not in foreign) )
                new_data[table] = []
                new_data_ign[table] = []
                if len(keys) > 0:
                    sql = sql[:-1] + ', PRIMARY KEY (%s))'%','.join(keys)

                print(sql)
                c.execute(sql)
                if len(keys) > 0:
                    #c.execute('DROP INDEX IF EXISTS %s_%s_index'%(table,'_'.join(keys)))
                    sql = 'CREATE INDEX IF NOT EXISTS %s_%s_index ON %s (%s);'%(table,'_'.join(keys),table, ','.join(keys))
                    print(sql)
                    c.execute(sql)

                for each in indexes:
                    #c.execute('DROP INDEX IF EXISTS %s_%s_index'%(table,each))
                    sql = 'CREATE INDEX IF NOT EXISTS %s_%s_index ON %s (%s);'%(table,each,table, each)
                    print(sql)
                    c.execute(sql)


                for f_table,f_keys in foreign.items():
                    if not isinstance(f_keys,(list,tuple,set)):
                        f_keys = [f_keys]
                    f_keys = set(f_keys)
                    if f_table in example:
                        if f_table in tables:
                            continue
                        if drop:
                            c.execute('DROP TABLE IF EXISTS %s'%f_table)

                        ex = example[f_table]
                        sql = 'CREATE TABLE IF NOT EXISTS %s(%s)'%(f_table, ', '.join(  _field(k,v,k in f_keys) for k,v in ex.items()) )
                        new_data[f_table] = []
                        new_data_ign[f_table] = []
                        print(sql)
                        c.execute(sql)
                        if len(f_keys) > 0:
                            #db.execute('DROP INDEX IF EXISTS %s_%s_index'%(f_table,'_'.join(f_keys)))
                            sql = 'CREATE INDEX IF NOT EXISTS %s_%s_index ON %s (%s);'%(f_table,'_'.join(f_keys),f_table, ','.join(f_keys))
                            print(sql)
                            c.execute(sql)
        finally:
            try:
                c.execute('COMMIT')
            except Exception as e:
                pass
            
            c.close()
            
        db.row_factory = dict_factory
        looper = Thread(target=self.looper_func, args=())
        looper.daemon = True
        looper.start()

    def put(self, table, value, ignore=False):
        new_data = self.new_data
        if ignore == True:
            new_data = self.new_data_ign
            
        if table not in new_data:
            raise Exception('No table named %s'%table)
            
        (new_data[table].extend if isinstance(value,list) else new_data[table].append)(value)
        
        
    
    def delete(self, table, value, ignore_duplicate=False, lock=True, db=None):
        db = db or self.db
        update_lock = self.update_lock
        try:
            items = list(value.items())
            sql = '''DELETE FROM %s WHERE %s;'''%(table, ' AND '.join('`'+k+'` = ?' for k,v in items) )
            if lock:
                with update_lock:
                    c = db.cursor()
                    try:
                        while True:
                            try:
                                c.execute('BEGIN')
                                break
                            except:
                                sleep(0.2)
                                
                        try:
                            c.execute(sql,tuple([v for k,v in items]))
                        except Exception as e:
                            import traceback
                            print(e)
                            traceback.print_exc()
                            raise e
                    finally:

                        try:
                            c.execute('COMMIT')
                        except Exception as e:
                            pass
                        c.close()
            else:
                db.execute(sql,tuple([v for k,v in items]))

        except Exception as e:
            import traceback
            print('delete', e)
            traceback.print_exc()


    
    def update(self, 
               table, 
               value, 
               ignore_duplicate=False, 
               lock=True, db=None):
        ignore_invalid = self.ignore_invalid
        db = db or self.db
        update_lock = self.update_lock
        tables = self.tables
        foreign = self.foreign
        try:
            items = list(value.items())

            for k,v in items:
                if isinstance(v,datetime):
                    v = v.timestamp()
                elif isinstance(v,(int,float,bool,str,bytes)):
                    continue

                elif isinstance(v,dict):
                    if k in foreign:
                        key = foreign[k]
                        self.update(k,v, ignore_duplicate=ignore_duplicate, lock=lock, db=db)
                        value[k] = v[key]
                        continue
                    else:
                        #value[k] = json.dumps(v)
                        continue
                elif isinstance(v,list):
                    #value[k] = json.dumps(v)
                    continue
                    

                del value[k]

            items = list(value.items())
            keys = (tables[table]['keys'] if 'keys' in tables[table] else []) if table in tables else foreign[table]
            if not isinstance(keys,(list,tuple,set)):
                keys = [keys]
            keys = set(keys)
            sql = '''INSERT OR ''' + ('IGNORE' if ignore_duplicate else 'REPLACE' ) + ''' INTO %s(%s) VALUES (%s) '''%(table, 
                                                   ','.join('`'+k+'`' for k,v in items),
                                                   ','.join('?'*len(items))

                                                )
            #print(sql)
            if lock:
                with update_lock:
                    c = db.cursor()
                    try:
                        while True:
                            try:
                                c.execute('BEGIN')
                                break
                            except:
                                sleep(0.2)
                                
                        try:
                            c.execute(sql,tuple([json.dumps(v) if isinstance(v,(dict,list)) else v for k,v in items]))
                        except Exception as e:
                            import traceback
                            print(e)
                            traceback.print_exc()
                            raise e
                    finally:

                        try:
                            c.execute('COMMIT')
                        except Exception as e:
                            pass
                        c.close()
            else:
                db.execute(sql,tuple([json.dumps(v) if isinstance(v,(dict,list)) else v for k,v in items]))
        except Exception as e:
            print('db update failed ! >>', e)
            if not ignore_invalid:
                if lock:
                    with update_lock:
                        c = db.cursor()
                        try:
                            while True:
                                try:
                                    c.execute('BEGIN')
                                    break
                                except:
                                    sleep(0.2)

                            try:
                                self.add_columns_to_table(c,table,value)
                            except Exception as e:
                                import traceback
                                print(e)
                                traceback.print_exc()
                                raise e
                        finally:

                            try:
                                c.execute('COMMIT')
                            except Exception as e:
                                pass
                            c.close()
                else:
                    self.add_columns_to_table(db,table,value)

                self.update(table,value, ignore_duplicate=ignore_duplicate, lock=lock)

    def batch_delete(self, table, values, ignore_duplicate=False):
        db = self.db
        update_lock = self.update_lock
        with update_lock:
            c = db.cursor()
            try:
                while True:
                    try:
                        c.execute('BEGIN')
                        break
                    except:
                        sleep(0.2)

                try:
                    for value in values:
                        try:
                            self.delete(table, value, ignore_duplicate=ignore_duplicate, lock=False, db=c)
                        except Exception as e:
                            import traceback
                            print(e)
                            traceback.print_exc()
                except Exception as e:
                    import traceback
                    print(e)
                    traceback.print_exc()
                    raise e
            finally:

                try:
                    c.execute('COMMIT')
                except Exception as e:
                    pass
                c.close()

    def batch_update(self, table, values, ignore_duplicate=False):
        db = self.db
        update_lock = self.update_lock
        with update_lock:
            c = db.cursor()
            try:
                while True:
                    try:
                        c.execute('BEGIN')
                        break
                    except:
                        sleep(0.2)
                try:
                    for value in values:
                        try:
                            self.update(table, value, ignore_duplicate=ignore_duplicate, lock=False, db=c)
                        except Exception as e:
                            import traceback
                            print(e)
                            traceback.print_exc()
                except Exception as e:
                    import traceback
                    print(e)
                    traceback.print_exc()
                    raise e
            finally:

                try:
                    c.execute('COMMIT')
                except Exception as e:
                    pass
                c.close()
                
                
    def add_columns_to_table(self, c, table_name, value):
        print('adding column to table `%s`'%table_name + '\n', value)
        db = self.db
        temp = db.row_factory
        c.row_factory = sqlite3.Row
        
        table_fields = [row[1] for row in c.execute('PRAGMA table_info({})'.format(table_name))]
        for k, v in value.items():
            for row in table_fields:
                if row == k:
                    break
            else:
                c.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table_name, k, get_sqlite3_type(v)))
        c.row_factory = temp

        
    def looper_func(self):
        update_lock = self.update_lock
        new_data = self.new_data
        new_data_ign = self.new_data_ign
        batch_update = self.batch_update
        tables = self.tables
        while True:
            try:
                sleep(0.1)
                to_be_updates = {}
                with update_lock:
                    for table, vals in new_data.items():
                        to_be_updates[table] = vals[:]
                        new_data[table] = []

                for table, vals in to_be_updates.items():
                    if len(vals) == 0 :
                        continue
                    if len(vals) > 0:
                        print('updating db... (%s)'%table)
                    batch_update(table, vals, ignore_duplicate=tables[table]['duplicate'] == 'ignore' if 'duplicate' in tables[table] else False)

                to_be_updates = {}
                with update_lock:
                    for table, vals in new_data_ign.items():
                        to_be_updates[table] = vals[:]
                        new_data_ign[table] = []

                for table, vals in to_be_updates.items():
                    if len(vals) == 0 :
                        continue
                    if len(vals) > 0:
                        print('updating db... (%s)'%table)
                    batch_update(table, vals, ignore_duplicate=True)


            except Exception as e:
                import traceback
                traceback.print_exc()

    def execute(self,*args,**kwargs):
        return self.db.execute(*args,**kwargs)
    
    
    

    