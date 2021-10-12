from gevent import monkey;monkey.patch_all()
import gevent
import os
import re
import time
import utm
import simplekml
import pandas as pd
import random

class V2X_Proxy(object):
    """这是红绿灯信号覆盖脚本类"""

    def __init__(self, utmid, ids):
        self.ids = ids  # 填写路口id
        self.utmid = utmid  # 选择对应地区的UTM
        self.file_path = '/Users/v_fanweiwei01/Desktop/Script/log/proxy/'  # 红绿灯文件的路径
        self.file_re = '^v2x-proxy.log.'+time.strftime("%Y%m%d")  # 正则匹配今天的日志数据
        self.date_list = [] #接收到所有位置信息
        self.delays = [] #接收到的路口时延信息
        self.time_one=[] #接收到所有路口信息时间

    def Feils(self):
        """提取文件绝对路径储存到列表方法"""
        files_path = []
        files = os.listdir(self.file_path)
        files.sort()
        for file in files:
            if re.search(self.file_re, file):
                files_path.append(self.file_path+file)
        if not files_path:
            return
        else:
            return files_path

    def utmll(self, x, y):
        """通过utm转换坐标点方法"""
        lat, lon = utm.to_latlon(x, y, self.utmid, northern=True)
        return lon, lat

    def Read_feils(self, file):
        """遍历每个文件提取相应的有效数据方法"""

        with open(file) as f:
            while True:  # 循环每行文件内容
                line = f.readline()
                if "<v2x-proxy> Same Juncion: %s" % self.ids in line:
                    if line.split()[7] == self.ids:  # 当路口为数为个位时容易匹配到高位数，通过判断控制
                        self.time_one.append(line.split()[1][0:8])
                    # if "<v2x-proxy> This RSU lijia#%s is in the white list"% self.ids in line:
                    #     if line.split()[7].split('#')[1]==self.ids:#当路口为数为个位时容易匹配到高位数，通过判断控制
                elif "<v2x-proxy> vehicle localization point" in line:
                    time_two = line.split()[1][0:8]
                    x = line.split('point(')[1].split(', ')[0]
                    y = line.split(', ')[1].split(')\n')[0]
                    if x=='nan' or y=='nan':
                        continue
                    lon, lat = self.utmll(float(x), float(y))
                    # print("正在提炼"+time_two,lon, lat,random.randint(0,30)*'>'+"done")   
                    self.date_list.append([time_two, lon, lat]) # 提取有效数据添加二维列表供pandas使用
                elif "<v2x-proxy> Timestamp Delta (Junction: %s):" % self.ids in line: # 筛选路口延迟并打印在控制台
                    ti = int(float(line.split()[9])*1000)  # 获取延时并转化
                    self.delays.append(ti)

                elif not line:
                    break

    def kml_id(self):
        """"通过pandas储存数据并写入到KML方法"""
        data1 = pd.DataFrame(self.time_one, columns=["times"])  # 使用pandas储存时间
        data1['hz'] = data1.groupby('times').times.transform('count')  # 按照时间统计每秒接收的频率并追加后一列
        df1 = data1.groupby('times').mean()  # 分组去重时间平均数据
        data2 = pd.DataFrame(self.date_list, columns=["times", "lon", "lat"])  # 使用pandas储存时间比对后的位置
        df2 = data2.groupby('times').mean()  # 分组去重平均数据
        df_inner=pd.merge(df1,df2,on='times') #合并两个表取出有用数据

        kml = simplekml.Kml()
        for row in df_inner.iterrows():  # 通过pandas结果集遍历每一行数据
            # print(row[0],row[1][0],row[1][1],row[1][2])
            pnt = kml.newpoint(name=int(row[1][0]), coords=[(row[1][1], row[1][2])])  # 把坐标写入到KML中
            pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/square.png'  # 设置KML方块样式
            pnt.style.iconstyle.scale = 0.4  # 设置KML方块样式大小
            pnt.style.labelstyle.scale=0.5 #设置标签字体大小

            if row[1][0] >= 8:  # 根据频率大小改变方块颜色
                pnt.style.iconstyle.color = 'ff32cd32'  # green
            elif row[1][0] >= 4 and row[1][0] <= 7:
                pnt.style.iconstyle.color = 'ffff00ff'  # magenta
            elif row[1][0] < 4:
                pnt.style.iconstyle.color = 'ff1600fc'  # red
        kml.save(self.file_path+"traffic_%s.kml" % self.ids)  # 生成KML文件

    def delay_date(self):
        """这是计算50、90、99分位的方法并打印到终端"""
        
        data = pd.DataFrame({self.ids: self.delays}) #写入pandas
        delay_50 = data[self.ids].quantile(0.50) #计算50分位数
        delay_90 = data[self.ids].quantile(0.90) #计算90分位数
        delay_99 = data[self.ids].quantile(0.99) #计算99分位数
        # self.delays.sort()
        # print(self.delays)
        print("%s号路口50分位: %s"%(self.ids, int(delay_50))+"ms")
        print("%s号路口90分位: %s"%(self.ids, int(delay_90))+"ms")
        print("%s号路口99分位: %s"%(self.ids, int(delay_99))+"ms")
       
    
if __name__ == '__main__':
    
        """程序入口设置对应城市的UTM"""
        citys={
            "yizhuang": 50,
            "guangzhou": 49,
            "chongqing": 48,
            "changsha": 48
        }
        city=input("请输城市名称(chongqing,changsha,guangzhou,yizhuang):")
        if city in citys.keys():
            utmid=citys[city]
            print(city, ">>>>>>----------------》》》》", citys[city])
            while True:
                ids=input('请输入路口号^_^:---!回车直接退出哦!---:')
                if not ids:
                    break
                else:
                    try:
                        time1=time.time()
                        prox=V2X_Proxy(utmid, ids)  # 创建类对象
                    
                        gevent_s=[gevent.spawn(prox.Read_feils, file) for file in prox.Feils()]  # 通过文件列表创建gevent列表
                        gevent.joinall(gevent_s)  # 添加到gevent
                        prox.kml_id()  # 调用生成KML方法
                        prox.delay_date() # 调用生成时延方法
                        time2=time.time()
                        print("总共用时:",time2-time1)
                        
                    except Exception as f:
                        print("请查阅是否有今天>>>>>>>>>>>>>>>>>>>>>>>>>> %s--%s.log" %(city, time.strftime("%Y%m%d")))

        else:
            print("citys >>>>> 么有该地区^_^!!!请检查!!!")

