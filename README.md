# ETL
# ПРОЦЕСС ЗАГРУЗКИ ДАННЫХ И ВЫЯВЛЕНИЕ МОШЕННИЧЕСКИХ ОПЕРАЦИЙ
import pandas as pd
import jaydebeapi
from datetime import datetime

connect = jaydebeapi.connect(
  'oracle.jdbc.driver.OracleDriver',
  'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
  ['de3hd','bilbobaggins'],
  'ojdbc7.jar')

cursor = connect.cursor()



def init_trnsctns():
  cursor.execute('''
    CREATE TABLE if not exists de3hd.s_21_trnsctns(
      trans_id varchar(128),
      trans_date timestamp,
      amount decimal,
      card_num varchar(128),
      oper_type varchar(128),
      oper_result varchar(128),
      terminal varchar(128),
      ceate_dt timestamp default TRUNC(SYSDATE - 1),
      update_dt timestamp default TRUNC(SYSDATE) - INTERVAL '1' SECOND
    )
  ''')



def init_terminals():
  cursor.execute('''
    CREATE TABLE if not exists de3hd.s_21_trmnls(
      terminal_id varchar(128),
      terminal_type varchar(128),
      terminal_city varchar(128),
      terminal_address varchar(128)
    )
  ''')




def init_bl_pasport():
  cursor.execute('''
    CREATE TABLE if not exists de3hd.s_21_pssprt_blclst(
      entry_dt date,
      passport_num varchar(128),
      deleted_flg integer default 0,
      effective_from_dttm date default (to_date('2021-03-01', 'YYYY.MM.DD')),
      effective_to_dttm date default (to_date('2999-12-31', 'YYYY.MM.DD'))
    )
  ''')





def init_table_real():
  try:
    cursor.execute('''
      CREATE TABLE de3hd.s_21_real_trnsctns(
        trans_id varchar(128),
        trans_date timestamp,
        amount decimal,
        card_num varchar(128),
        oper_type varchar(128),
        oper_result varchar(128),
        terminal varchar(128),
        create_dt timestamp,
        update_dt timestamp
      )
    ''')
  except jaydebeapi.DatabaseError:
    print('- Такая таблица уже есть')


def init_tables_dwh_dim():
  try:
    cursor.execute('''
      CREATE TABLE de3hd.s_21_dwh_cards(
        card_num varchar(128),
        account_num varchar(128),
        create_dt date,
        update_dt date
      )
    ''')

    cursor.execute('''
      INSERT INTO de3hd.s_21_dwh_cards
      SELECT *
      FROM BANK.cards
    ''')
  except jaydebeapi.DatabaseError:
    print('- Такая таблица уже есть')


  try:
    cursor.execute('''
      CREATE TABLE de3hd.s_21_dwh_clients(
        account_num varchar(128),
        valid_to date,
        client varchar(128),
        create_dt date,
        update_dt date
      )
    ''')

    cursor.execute('''
      INSERT INTO de3hd.s_21_dwh_clients
      SELECT *
      FROM BANK.accounts
    ''')    
  except jaydebeapi.DatabaseError:
    print('- Такая таблица уже есть')

  try:
    cursor.execute('''
      CREATE TABLE de3hd.s_21_dwh_clients(
        client_id varchar(128),
        last_name varchar(128),
        first_name varchar(128),
        patrinymic varchar(128),
        date_of_birth date,
        passport_num varchar(128),
        passport_valid_to date,
        phone varchar(128),
        create_dt date,
        update_dt date
      )
    ''')

    cursor.execute('''
      INSERT INTO de3hd.s_21_dwh_clients
      SELECT *
      FROM BANK.clients
    ''')
  except jaydebeapi.DatabaseError:
    print('- Такая таблица уже есть')



def init_tables_dwh_dim_hist():
  try:
    cursor.execute('''
      CREATE TABLE de3hd.s_21_dwh_dim_pssprt_bl_hist(
        entry_dt date,
        passport_num varchar(128),
        deleted_flg integer default 0,
        effective_from_dttm date default (to_date('2021-03-01', 'YYYY.MM.DD')),
        effective_to_dttm date default (to_date('2999-12-31', 'YYYY.MM.DD'))
      )
    ''')
  except jaydebeapi.DatabaseError:
    print('- Такая таблица уже есть')


def init_report():  

  try:
    cursor.execute('''
      CREATE TABLE de3hd.s_21_rep_fraund(
        event_dt timestamp,
        passport varchar(128),
        fio varchar(128),
        phone varchar(128),
        event_type varchar(128),
        report_dt timestamp 
      )
    ''')
  except jaydebeapi.DatabaseError:
    print('- Такая таблица уже есть')

  cursor.execute('''
    CREATE TABLE de3hd.s_21_rep_fraund_new(
      event_dt timestamp,
      passport varchar(128),
      fio varchar(128),
      phone varchar(128),
      event_type varchar(128),
      report_dt timestamp default current_timestamp
    )
  ''')
