#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 11:28:51 2018

@author: areed145

"""

from queue import Queue
from threading import Thread
import pandas as pd
import numpy as np
import requests
import re
import datetime
from pymongo import MongoClient
import os

"""
ftp://ftp.consrv.ca.gov/pub/oil/Online_Data/Production_Injection_Data/2018CaliforniaOilAndGasWells.csv
ftp://ftp.consrv.ca.gov/pub/oil/Online_Data/Production_Injection_Data/2018CaliforniaOilAndGasWellMonthlyProduction.csv
ftp://ftp.consrv.ca.gov/pub/oil/Online_Data/Production_Injection_Data/2018CaliforniaOilAndGasWellMonthlyInjection.csv
ftp://ftp.consrv.ca.gov/pub/oil/Online_Data/Production_Injection_Data/2019CaliforniaOilAndGasWells.csv
ftp://ftp.consrv.ca.gov/pub/oil/Online_Data/Production_Injection_Data/2019CaliforniaOilAndGasWellMonthlyProduction.csv
ftp://ftp.consrv.ca.gov/pub/oil/Online_Data/Production_Injection_Data/2019CaliforniaOilAndGasWellMonthlyInjection.csv
"""

d = pd.read_csv("AllWells_20180131.csv")
apis = d["API"].copy(deep=True)
apis.sort_values(inplace=True, ascending=True)
apistodo = apis
# apistodo = apis[(apis >= 0) & (apis <= 28000000)]
apistodo = apistodo.astype("str").to_list()

client = MongoClient(os.environ["MONGODB_CLIENT"])
db = client.petroleum
exists = pd.DataFrame(list(db.doggr.find({}, {"api": 1})))
exists = exists["api"].astype("int").astype("str").to_list()

print(exists[:10])
print(apistodo[:10])

for well in exists:
    try:
        apistodo.remove(well)
    except Exception:
        pass


class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            api, percent, ct = self.queue.get()
            url = (
                "https://secure.conservation.ca.gov/WellSearch/Details?api="
                + "{num:08d}".format(num=int(api))
            )
            print(url + ", " + str(ct) + ", " + str(percent))
            page = requests.get(url).text
            lease = re.findall("Lease</label> <br />\s*(.*?)\s*</div", page)[0]
            well = re.findall("Well #</label> <br />\s*(.*?)\s*</div", page)[0]
            county = re.findall(
                "County</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>", page
            )[0][0]
            countycode = re.findall(
                "County</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>", page
            )[0][1]
            district = int(
                re.findall("District</label> <br />\s*(.*?)\s*</div", page)[0]
            )
            operator = re.findall(
                "Operator</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>",
                page,
            )[0][0]
            operatorcode = re.findall(
                "Operator</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>",
                page,
            )[0][1]
            field = re.findall(
                "Field</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>", page
            )[0][0]
            fieldcode = re.findall(
                "Field</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>", page
            )[0][1]
            area = re.findall(
                "Area</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>", page
            )[0][0]
            areacode = re.findall(
                "Area</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>", page
            )[0][1]
            section = re.findall(
                "Section</label><br />\s*(.*?)\s*</div", page
            )[0]
            township = re.findall(
                "Township</label><br />\s*(.*?)\s*</div", page
            )[0]
            rnge = re.findall("Range</label><br />\s*(.*?)\s*</div", page)[0]
            bm = re.findall(
                "Base Meridian</label><br />\s*(.*?)\s*</div", page
            )[0]
            wellstatus = re.findall(
                "Well Status</label><br />\s*(.*?)\s*</div", page
            )[0]
            pwt = re.findall(
                "Pool WellTypes</label> <br />\s*(.*?)\s*</div", page
            )[0]
            spuddate = re.findall(
                "SPUD Date</label> <br />\s*(.*?)\s*</div", page
            )[0]
            gissrc = re.findall(
                "GIS Source</label> <br />\s*(.*?)\s*</div", page
            )[0]
            elev = re.findall("Datum</label> <br />\s*(.*?)\s*</div", page)[0]
            latitude = re.findall(
                "Latitude</label> <br />\s*(.*?)\s*</div", page
            )[0]
            longitude = re.findall(
                "Longitude</label> <br />\s*(.*?)\s*</div", page
            )[0]
            hh = {}
            hh["lease"] = lease
            hh["well"] = well
            hh["county"] = county
            hh["countycode"] = countycode
            hh["district"] = district
            hh["operator"] = operator
            hh["operatorcode"] = operatorcode
            hh["field"] = field
            hh["fieldcode"] = fieldcode
            hh["area"] = area
            hh["areacode"] = areacode
            hh["section"] = section
            hh["township"] = township
            hh["rnge"] = rnge
            hh["bm"] = bm
            hh["wellstatus"] = wellstatus
            hh["pwt"] = pwt
            if spuddate == "":
                hh["spuddate"] = spuddate
            else:
                hh["spuddate"] = pd.to_datetime(spuddate, errors="ignore")
            hh["gissrc"] = gissrc
            try:
                hh["elev"] = float(elev)
            except Exception:
                hh["elev"] = elev
            try:
                hh["latitude"] = float(latitude)
            except Exception:
                hh["latitude"] = latitude
            try:
                hh["longitude"] = float(longitude)
            except Exception:
                hh["longitude"] = longitude
            hh["api"] = "{num:08d}".format(num=int(api))
            prod = re.findall('{"Production+(.*?)}', page)
            pp = pd.DataFrame()
            for idx, i in enumerate(prod):
                p = pd.DataFrame(index=[re.findall("Date\(+(.*?)\)", i)[0]])
                if len(prod) > 0:
                    p["date"] = datetime.datetime.fromtimestamp(
                        int(re.findall("Date\(+(.*?)\)", i)[0][:-3])
                    ).strftime("%Y-%m-%d")
                    p["oil"] = re.findall('OilProduced":+(.*?),', i)[0]
                    p["water"] = re.findall('WaterProduced":+(.*?),', i)[0]
                    p["gas"] = re.findall('GasProduced":+(.*?),', i)[0]
                    p["daysprod"] = re.findall(
                        'NumberOfDaysProduced":+(.*?),', i
                    )[0]
                    p["oilgrav"] = re.findall('OilGravity":+(.*?),', i)[0]
                    p["pcsg"] = re.findall('CasingPressure":+(.*?),', i)[0]
                    p["ptbg"] = re.findall('TubingPressure":+(.*?),', i)[0]
                    p["btu"] = re.findall('BTU":+(.*?),', i)[0]
                    p["method"] = re.findall('MethodOfOperation":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    p["waterdisp"] = re.findall(
                        'WaterDisposition":+(.*?),', i
                    )[0].replace('"', "")
                    p["pwtstatus_p"] = re.findall('PWTStatus":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    p["welltype_p"] = re.findall('WellType":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    p["status_p"] = re.findall('Status":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    p["poolcode_p"] = re.findall('PoolCode":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    if re.findall('YearlySum":+(.*?),', i)[0] != "true":
                        pp = pp.append(p).replace("null", np.nan, regex=True)
            if len(pp) > 0:
                pp["date"] = pd.to_datetime(pp["date"])
                pp["date"] = pp["date"].fillna("")
                for col in [
                    "oil",
                    "water",
                    "gas",
                    "daysprod",
                    "oilgrav",
                    "pcsg",
                    "ptbg",
                    "btu",
                ]:
                    pp[col] = pd.to_numeric(pp[col])
                ps = []
                for idx, row in pp.iterrows():
                    ps.append(row.to_dict())
                hh["prod"] = ps
            inj = re.findall('{"Injection+(.*?)}', page)
            jj = pd.DataFrame()
            for idx, i in enumerate(inj):
                j = pd.DataFrame(index=[re.findall("Date\(+(.*?)\)", i)[0]])
                if len(inj) > 0:
                    j["date"] = datetime.datetime.fromtimestamp(
                        int(re.findall("Date\(+(.*?)\)", i)[0][:-3])
                    ).strftime("%Y-%m-%d")
                    j["wtrstm"] = re.findall(
                        'WaterOrSteamInjected":+(.*?),', i
                    )[0]
                    j["gasair"] = re.findall('GasOrAirInjected":+(.*?),', i)[0]
                    j["daysinj"] = re.findall(
                        'NumberOfDaysInjected":+(.*?),', i
                    )[0]
                    j["pinjsurf"] = re.findall(
                        'SurfaceInjectionPressure":+(.*?),', i
                    )[0]
                    j["wtrsrc"] = re.findall('SourceOfWater":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    j["wtrknd"] = re.findall('KindOfWater":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    j["pwtstatus_i"] = re.findall('PWTStatus":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    j["welltype_i"] = re.findall('WellType":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    j["status_i"] = re.findall('Status":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    j["poolcode_i"] = re.findall('PoolCode":+(.*?),', i)[
                        0
                    ].replace('"', "")
                    if re.findall('YearlySum":+(.*?),', i)[0] != "true":
                        jj = jj.append(j).replace("null", np.nan, regex=True)
            if len(jj) > 0:
                jj["date"] = pd.to_datetime(jj["date"])
                jj["date"] = jj["date"].fillna("")
                for col in ["wtrstm", "gasair", "daysinj", "pinjsurf"]:
                    jj[col] = pd.to_numeric(jj[col])
                js = []
                for idx, row in jj.iterrows():
                    js.append(row.to_dict())
                hh["inj"] = js

            client = MongoClient(os.environ["MONGODB_CLIENT"])
            db = client.petroleum
            doggr = db.doggr

            try:
                doggr.insert_one(hh)
                print(str(api) + " succeeded")
            except Exception:
                print(str(api) + " failed")

            self.queue.task_done()


def main():
    queue = Queue()
    for x in range(20):
        worker = DownloadWorker(queue)
        worker.daemon = True
        worker.start()
    for idx, api in enumerate(apistodo):
        percent = np.round(100 * idx / len(apistodo), 2)
        queue.put((api, percent, idx + 1))
    queue.join()


main()
