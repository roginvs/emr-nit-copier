# -*- coding: cp1251 -*-
import xml.etree.ElementTree
import gzip
import os
import os.path
import sys
import ftplib
import datetime

# Скрипт копирует NIT таблицу с одного транспондера на указанные
# Проверено на EMR3.0 V3.0.3.12

# Нумерация транспондеров в конфигурации идёт с нуля, т.е. откуда копировать - Card1Port1
source_card = 'card1'
source_channel = 0
destinations = {'card1':[1,2,3,4,5,6,7],'card4':[0,1,2,3,4,5,6,7]}
#destinations = {'card1':[6,7],}

bkp_dir='backup'
tmp_dir='tmp'
sumavision_ip = '192.168.127.220'
sumavision_login = 'target'
sumavision_password = 'target'


def check_directories():
  for x in (bkp_dir,tmp_dir):
   if not os.path.isdir(x):
      sys.stdout.write(u'Создаём каталог %s.. ' % x)
      os.mkdir(x)
      sys.stdout.write(u'ok\n')

def _get_cards_in_action():
    cards_in_action = destinations.keys()
    cards_in_action.append(source_card)
    return list(set(cards_in_action))

def download_config_to_backup():
    sys.stdout.write(u'Скачиваем конфигурацию %s:\n' % backup_id)
    
    os.mkdir(os.path.join(bkp_dir,backup_id))
    os.mkdir(os.path.join(bkp_dir,backup_id,'para'))
    ftp = ftplib.FTP(sumavision_ip,sumavision_login,sumavision_password)
    ftp.cwd('para')
    for fname in ftp.nlst():
        if fname == '.' or fname == '..':
            continue
        sys.stdout.write(u'  %s.. ' % fname)
        download_file = open(os.path.join(bkp_dir,backup_id,'para',fname),'wb')
        ftp.retrbinary('RETR %s' % fname, download_file.write)
        download_file.close()
        sys.stdout.write(u'ok\n')
    
def upload_cards_config_to_emr():
   sys.stdout.write(u'Закачиваем конфигурацию карт на EMR:\n')
   ftp = ftplib.FTP(sumavision_ip,sumavision_login,sumavision_password)
   ftp.cwd('para')
   for card in _get_cards_in_action():
     sys.stdout.write(u'  %s.. ' % card)     
     file_to_upload = open(os.path.join(tmp_dir,backup_id,"%s.xml.gz" % card),'rb')
     ftp.storbinary('STOR %s.xml.gz' % card, file_to_upload)
     file_to_upload.seek(0)
     ftp.storbinary('STOR %s.xml.bak.gz' % card, file_to_upload) # Похоже что EMR нужны оба файла
     file_to_upload.close()
     sys.stdout.write(u'ok\n')


def unpack_cards():
    sys.stdout.write(u'Распаковываем xml.. ')
    os.mkdir(os.path.join(tmp_dir,backup_id))
    for card in _get_cards_in_action():
        sfname = os.path.join(bkp_dir,backup_id,'para',"%s.xml.gz" % card)        
        dfname = os.path.join(tmp_dir,backup_id,"%s.xml" % card)        
        f_in = gzip.open(sfname,'rb')
        f_out = open(dfname,'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
    sys.stdout.write(u'ok\n')
    
def pack_cards():
    sys.stdout.write(u'Архивируем xml.. ')
    for card in _get_cards_in_action():
        sfname = os.path.join(tmp_dir,backup_id,"%s.xml" % card)        
        dfname = os.path.join(tmp_dir,backup_id,"%s.xml.gz" % card)                
        f_in = open(sfname,'rb')
        f_out = gzip.open(dfname,'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
    sys.stdout.write(u'ok\n')
    
def get_source_nit():
    #qam8Param/qam8PortPara/idx7/psiPkt/idx0/pid
    sys.stdout.write(u'Достаём NIT таблицу с карты %s, транспондер %s.. ' % (source_card,source_channel))
    tree = xml.etree.ElementTree.parse(os.path.join(tmp_dir,backup_id,"%s.xml" % source_card))
    root = tree.getroot()    
    source_nit = []
    for x in root.find('qam8PortPara').find('idx%s' % source_channel).find('psiPkt'):
        if x.find('pid').text == '0x10' and x.find('psiType').text == '4':
            source_nit.append(x)
    sys.stdout.write(u'ok\n')        
    return source_nit

def fix_nit_and_fix_ids():
    sys.stdout.write(u'Вписываем новую NIT:\n')
    for card in destinations.keys():
      card_fname = os.path.join(tmp_dir,backup_id,"%s.xml" % card)
      tree = xml.etree.ElementTree.parse(card_fname)
      os.rename(card_fname,"%s.orig.xml" % card_fname)
      
      
      root = tree.getroot()
      sys.stdout.write (u'  Редактируем %s:\n' % card)
      for channel in destinations[card]:
        sys.stdout.write(u"    Транспондер %s:\n" % channel)
        psiPkt = root.find('qam8PortPara').find('idx%s' % channel).find('psiPkt') 
        if psiPkt:
          to_remove = []
          for x in psiPkt:
            if x.find('pid').text == '0x10' and x.find('psiType').text == '4':
             to_remove.append(x)
          for x in to_remove:
            sys.stdout.write(u"      Удаляем старый элемент %s.. " % x.tag)  
            psiPkt.remove(x)
            sys.stdout.write(u"ok\n")                       
          for x in source_nit:
            sys.stdout.write(u"      Добавляем новый элемент %s.. " % x.tag)
            #Это чтобы ссылки были на разные экземпляры и далее правка tag-а в одном месте не влияла на другой
            new_x = xml.etree.ElementTree.fromstring(xml.etree.ElementTree.tostring(x))            
            psiPkt.append(new_x)
            sys.stdout.write(u"ok\n")
            
          count = len(root.find('qam8PortPara').find('idx%s' % channel).find('psiPkt'))
          if int(root.find('qam8PortPara').find('idx%s' % channel).find('xml_psiPkt_length').text) != count:
              sys.stdout.write(u"      Корректируем тэг с количеством элементов.. ")
              root.find('qam8PortPara').find('idx%s' % channel).find('xml_psiPkt_length').text = "%s" % count
              sys.stdout.write(u"ok\n")

          i = 0
          for x in psiPkt:            
            new_tag = "idx%s" % i
            if x.tag != new_tag:
               sys.stdout.write(u"      Перенумеровываем элемент %s в %s.. " % (x.tag,new_tag))
               x.tag= new_tag
               sys.stdout.write(u"ok\n") 
            i += 1

#          for x in root.find('qam8PortPara').find('idx%s' % channel).find('psiPkt'):
#            print x
#            print x.find('pid').text

          
      sys.stdout.write(u'    Сохраняем.. ')
      tree.write(card_fname,'utf-8',True)
      sys.stdout.write(u'ok\n')


    
        
def delete_temp():
    sys.stdout.write(u'Удаляем временные файлы.. ')
    cur_tmp_dir = os.path.join(tmp_dir,backup_id)
    for fname in os.listdir(cur_tmp_dir):
        full_fname = os.path.join(cur_tmp_dir,fname)
        if os.path.isfile(full_fname):
            os.remove(full_fname)
    os.rmdir(cur_tmp_dir)
    sys.stdout.write(u'ok\n')


backup_id = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
check_directories()
download_config_to_backup()
unpack_cards()
source_nit = get_source_nit()
fix_nit_and_fix_ids()
pack_cards()
upload_cards_config_to_emr()
delete_temp() 
