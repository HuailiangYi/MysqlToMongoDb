from record.record import Record

if __name__ == '__main__':
    r = Record('test.db')

    print(r.findKeyExist('tb_vehicle_status_his_20_05_16'))
    r.updateHandledNum('tb_vehicle_status_his_20_05_16', 10)
    print(r.getHandledNum('tb_vehicle_status_his_20_05_16'))
    print(r.findKeyExist('tb_vehicle_status_his_20_05_16'))
    r.updateHandledNum('tb_vehicle_status_his_20_05_16', 20)
    print(r.getHandledNum('tb_vehicle_status_his_20_05_16'))

