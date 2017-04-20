#  coding: utf-8
class Aqi(object):
    def __init__(self):
        self.el = None

    @staticmethod
    def validate(val):
        """try to make float value from input and replace , to ."""
        try:
            _val = float(val)
            return _val
        except ValueError:
            _val = None

        return _val

    def get_max_aqi(self, _dict, show=False):
        """
        :return: максимальне значення aqi із домустимих забрудників
        """

        aqis = []
        for key in _dict:
            aqi = self.get_aqi(_dict[key], key)
            aqis.append(aqi)
            if show is True:
                print(key, _dict[key], 'aqi', aqi)
        return max(aqis)

    def val_to_aqi(self, val, v_type):
        #  i_low, i_high, c_low, c_high

        #  print('input value', val)

        if v_type == 'pm25':
            if 0.0 <= val <= 12.0:
                self.el = [0, 50, 0.0, 12.0]
    
            elif 12.1 <= val <= 35.4:
                self.el = [51, 100, 12.1, 35.4]
    
            elif 35.5 <= val <= 55.4:
                self.el = [101, 150, 35.5, 55.4]
    
            elif 55.5 <= val <= 150.4:
                self.el = [151, 200, 55.5, 150.4]
    
            elif 150.5 <= val <= 250.4:
                self.el = [201, 300, 150.5, 250.4]
    
            elif 250.5 <= val <= 350.4:
                self.el = [301, 400, 250.5, 350.4]
    
            elif 350.5 <= val <= 500.4:
                self.el = [401, 500, 350.5, 500.4]
    
            else:
                i = 501
                return i
    
        elif v_type == 'pm10':
            if 0.0 <= val <= 54.0:
                self.el = [0, 50, 0, 54]
    
            elif 55 <= val <= 154:
                self.el = [51, 100, 55, 154]
    
            elif 155 <= val <= 254:
                self.el = [101, 150, 155, 254]
    
            elif 255 <= val <= 354:
                self.el = [151, 200, 255, 354]
    
            elif 355 <= val <= 424:
                self.el = [201, 300, 355, 424]
    
            elif 425 <= val <= 504:
                self.el = [301, 400, 425, 504]
    
            elif 505 <= val <= 604:
                self.el = [401, 500, 505, 604]
    
            else:
                i = 501
                return i
    
        elif v_type == 'co':
            if 0 <= val <= 4.4:
                self.el = [0, 50, 0, 4.4]
    
            elif 4.5 <= val <= 9.4:
                self.el = [51, 100, 4.5, 9.4]
    
            elif 9.5 <= val <= 12.4:
                self.el = [101, 150, 9.5, 12.4]
    
            elif 12.5 <= val <= 15.4:
                self.el = [151, 200, 12.5, 15.4]
    
            elif 15.5 <= val <= 30.4:
                self.el = [201, 300, 15.5, 30.4]
    
            elif 30.5 <= val <= 40.4:
                self.el = [301, 400, 30.5, 40.4]
    
            elif 40.5 <= val <= 50.4:
                self.el = [401, 500, 40.5, 50.4]
    
            else:
                i = 501
                return i
    
        elif v_type == 'so2':
            if 0 <= val <= 35:
                self.el = [0, 50, 0, 35]
    
            elif 36 <= val <= 75:
                self.el = [51, 100, 36, 75]
    
            elif 76 <= val <= 185:
                self.el = [101, 150, 76, 185]
    
            elif 186 <= val <= 304:
                self.el = [151, 200, 186, 304]
    
            elif 305 <= val <= 604:
                self.el = [201, 300, 305, 604]
    
            elif 605 <= val <= 804:
                self.el = [301, 400, 605, 804]
    
            elif 805 <= val <= 1004:
                self.el = [401, 500, 805, 1004]
    
            else:
                i = 501
                return i
    
        elif v_type == 'no2':
            if 0 <= val <= 53:
                self.el = [0, 50, 0, 53]
    
            elif 54 <= val <= 100:
                self.el = [51, 100, 54, 100]
    
            elif 101 <= val <= 360:
                self.el = [101, 150, 101, 360]
    
            elif 361 <= val <= 649:
                self.el = [151, 200, 361, 649]
    
            elif 650 <= val <= 1249:
                self.el = [201, 300, 650, 1249]
    
            elif 1250 <= val <= 1649:
                self.el = [301, 400, 1250, 1649]
    
            elif 1650 <= val <= 2049:
                self.el = [401, 500, 1650, 2049]
    
            else:
                i = 501
                return i
    
        elif v_type == 'o3':
            if 0 <= val <= 54:
                self.el = [0, 50, 0, 54]
    
            elif 55 <= val <= 70:
                self.el = [51, 100, 55, 70]
    
            elif 71 <= val <= 85:
                self.el = [101, 150, 71, 85]
    
            elif 86 <= val <= 105:
                self.el = [151, 200, 86, 105]
    
            elif 106 <= val <= 200:
                self.el = [201, 300, 106, 200]
    
            elif 201 <= val <= 600:
                self.el = [301, 500, 201, 600]
    
            else:
                i = 501
                return i
    
        else:
            return False
    
        #  i = ((i_high - i_low)/(c_high - c_low)*(val - c_low)) + i_low
        i = (
            (float((self.el[1])) - float((self.el[0]))) /
            (float((self.el[3])) - float((self.el[2]))) *
            (val - float((self.el[2]))) +
            float(self.el[0])
        )
        i = round(i)
        i = int(i)

        return i

    def aqi_to_val(self, _aqi, v_type):
        #  i_low, i_high, c_low, c_high

        #  print('input value', val)
        if self.validate(_aqi):
            _aqi = self.validate(_aqi)

            if v_type == 'co':
                if 0.0 <= _aqi <= 50.9:
                    self.el = {
                        'i_low': 0.0,
                        'i_high': 50.0,
                        'c_low': 0.0,
                        'c_high': 4.4
                    }
                elif 51.0 <= _aqi <= 100.9:
                    self.el = {
                        'i_low': 51.0,
                        'i_high': 100.0,
                        'c_low': 4.5,
                        'c_high': 9.4
                    }
                elif 101.0 <= _aqi <= 150.9:
                    self.el = {
                        'i_low': 101.0,
                        'i_high': 150.0,
                        'c_low': 9.5,
                        'c_high': 12.4
                    }
                elif 151.0 <= _aqi <= 200.9:
                    self.el = {
                        'i_low': 151.0,
                        'i_high': 200.0,
                        'c_low': 12.5,
                        'c_high': 15.4
                    }
                elif 201.0 <= _aqi <= 300.9:
                    self.el = {
                        'i_low': 201.0,
                        'i_high': 300.0,
                        'c_low': 15.5,
                        'c_high': 30.4
                    }
                elif 301.0 <= _aqi <= 400.9:
                    self.el = {
                        'i_low': 301.0,
                        'i_high': 400.0,
                        'c_low': 30.5,
                        'c_high': 40.4
                    }
                elif 401.0 <= _aqi <= 500.0:
                    self.el = {
                        'i_low': 401.0,
                        'i_high': 500.0,
                        'c_low': 40.5,
                        'c_high': 50.4
                    }
                else:
                    return 9999
            elif v_type == 'o3':
                if 0.0 <= _aqi <= 50.9:
                    self.el = {
                        'i_low': 0.0,
                        'i_high': 50.0,
                        'c_low': 0.0,
                        'c_high': 54.0
                    }
                elif 51.0 <= _aqi <= 100.9:
                    self.el = {
                        'i_low': 51.0,
                        'i_high': 100.0,
                        'c_low': 55.0,
                        'c_high': 70.0
                    }
                elif 101.0 <= _aqi <= 150.9:
                    self.el = {
                        'i_low': 101.0,
                        'i_high': 150.0,
                        'c_low': 71.0,
                        'c_high': 85.0
                    }
                elif 151.0 <= _aqi <= 200.9:
                    self.el = {
                        'i_low': 151.0,
                        'i_high': 200.0,
                        'c_low': 86.0,
                        'c_high': 105.0
                    }
                elif 201.0 <= _aqi <= 300.9:
                    self.el = {
                        'i_low': 201.0,
                        'i_high': 300.0,
                        'c_low': 106.0,
                        'c_high': 200.0
                    }
                elif 301.0 <= _aqi <= 500.0:
                    self.el = {
                        'i_low': 301.0,
                        'i_high': 500.0,
                        'c_low': 201.0,
                        'c_high': 600.0
                    }
                else:
                    return 9999
            elif v_type == 'so2':
                if 0.0 <= _aqi <= 50.9:
                    self.el = {
                        'i_low': 0.0,
                        'i_high': 50.0,
                        'c_low': 0.0,
                        'c_high': 35.0
                    }
                elif 51.0 <= _aqi <= 100.9:
                    self.el = {
                        'i_low': 51.0,
                        'i_high': 100.0,
                        'c_low': 36.0,
                        'c_high': 75.0
                    }
                elif 101.0 <= _aqi <= 150.9:
                    self.el = {
                        'i_low': 101.0,
                        'i_high': 150.0,
                        'c_low': 76.0,
                        'c_high': 185.0
                    }
                elif 151.0 <= _aqi <= 200.9:
                    self.el = {
                        'i_low': 151.0,
                        'i_high': 200.0,
                        'c_low': 186.0,
                        'c_high': 304.0
                    }
                elif 201.0 <= _aqi <= 300.9:
                    self.el = {
                        'i_low': 201.0,
                        'i_high': 300.0,
                        'c_low': 305.0,
                        'c_high': 604.0
                    }
                elif 301.0 <= _aqi <= 400.9:
                    self.el = {
                        'i_low': 301.0,
                        'i_high': 400.0,
                        'c_low': 605.0,
                        'c_high': 804.0
                    }
                elif 401.0 <= _aqi <= 500.0:
                    self.el = {
                        'i_low': 401.0,
                        'i_high': 500.0,
                        'c_low': 805.0,
                        'c_high': 1004.0
                    }
                else:
                    return 9999
            elif v_type == 'no2':
                if 0.0 <= _aqi <= 50.9:
                    self.el = {
                        'i_low': 0.0,
                        'i_high': 50.0,
                        'c_low': 0.0,
                        'c_high': 53.0
                    }
                elif 51.0 <= _aqi <= 100.9:
                    self.el = {
                        'i_low': 51.0,
                        'i_high': 100.0,
                        'c_low': 54.0,
                        'c_high': 100.0
                    }
                elif 101.0 <= _aqi <= 150.9:
                    self.el = {
                        'i_low': 101.0,
                        'i_high': 150.0,
                        'c_low': 101.0,
                        'c_high': 360.0
                    }
                elif 151.0 <= _aqi <= 200.9:
                    self.el = {
                        'i_low': 151.0,
                        'i_high': 200.0,
                        'c_low': 361.0,
                        'c_high': 649.0
                    }
                elif 201.0 <= _aqi <= 300.9:
                    self.el = {
                        'i_low': 201.0,
                        'i_high': 300.0,
                        'c_low': 650.0,
                        'c_high': 1249.0
                    }
                elif 301.0 <= _aqi <= 400.9:
                    self.el = {
                        'i_low': 301.0,
                        'i_high': 400.0,
                        'c_low': 1250.0,
                        'c_high': 1649.0
                    }
                elif 401.0 <= _aqi <= 500.0:
                    self.el = {
                        'i_low': 401.0,
                        'i_high': 500.0,
                        'c_low': 1650.0,
                        'c_high': 2049.0
                    }
                else:
                    return 9999
            elif v_type == 'pm10':
                if 0.0 <= _aqi <= 50.9:
                    self.el = {
                        'i_low': 0.0,
                        'i_high': 50.0,
                        'c_low': 0.0,
                        'c_high': 54.0
                    }
                elif 51.0 <= _aqi <= 100.9:
                    self.el = {
                        'i_low': 51.0,
                        'i_high': 100.0,
                        'c_low': 55.0,
                        'c_high': 154.0
                    }
                elif 101.0 <= _aqi <= 150.9:
                    self.el = {
                        'i_low': 101.0,
                        'i_high': 150.0,
                        'c_low': 155.0,
                        'c_high': 254.0
                    }
                elif 151.0 <= _aqi <= 200.9:
                    self.el = {
                        'i_low': 151.0,
                        'i_high': 200.0,
                        'c_low': 255.0,
                        'c_high': 354.0
                    }
                elif 201.0 <= _aqi <= 300.9:
                    self.el = {
                        'i_low': 201.0,
                        'i_high': 300.0,
                        'c_low': 355.0,
                        'c_high': 424.0
                    }
                elif 301.0 <= _aqi <= 400.9:
                    self.el = {
                        'i_low': 301.0,
                        'i_high': 400.0,
                        'c_low': 425.0,
                        'c_high': 504.0
                    }
                elif 401.0 <= _aqi <= 500.0:
                    self.el = {
                        'i_low': 401.0,
                        'i_high': 500.0,
                        'c_low': 505.0,
                        'c_high': 604.0
                    }
                else:
                    return 9999
            elif v_type == 'pm25':
                if 0.0 <= _aqi <= 50.9:
                    self.el = {
                        'i_low': 0.0,
                        'i_high': 50.0,
                        'c_low': 0.0,
                        'c_high': 12.0
                    }
                elif 51.0 <= _aqi <= 100.9:
                    self.el = {
                        'i_low': 51.0,
                        'i_high': 100.0,
                        'c_low': 12.1,
                        'c_high': 35.4
                    }
                elif 101.0 <= _aqi <= 150.9:
                    self.el = {
                        'i_low': 101.0,
                        'i_high': 150.0,
                        'c_low': 35.5,
                        'c_high': 55.4
                    }
                elif 151.0 <= _aqi <= 200.9:
                    self.el = {
                        'i_low': 151.0,
                        'i_high': 200.0,
                        'c_low': 55.5,
                        'c_high': 150.4
                    }
                elif 201.0 <= _aqi <= 300.9:
                    self.el = {
                        'i_low': 201.0,
                        'i_high': 300.0,
                        'c_low': 150.5,
                        'c_high': 250.4
                    }
                elif 301.0 <= _aqi <= 400.9:
                    self.el = {
                        'i_low': 301.0,
                        'i_high': 400.0,
                        'c_low': 250.5,
                        'c_high': 350.4
                    }
                elif 401.0 <= _aqi <= 500.0:
                    self.el = {
                        'i_low': 401.0,
                        'i_high': 500.0,
                        'c_low': 350.5,
                        'c_high': 500.4
                    }
                else:
                    return 9999
            else:
                return False

            pol_value = (_aqi - self.el['i_low']) * ((self.el['c_high'] - self.el['c_low']) / (self.el['i_high'] - self.el['i_low'])) + self.el['c_low']

            return round(pol_value, 2)

if __name__ == '__main__':
    aqi = Aqi().val_to_aqi("\n", 'o3')
    print(aqi)
