import pandas as pd
import os
import yaml
import sqlite3

class compra:
    def __init__(self,ruta,rutabd):
           self.ruta = ruta
           self.rutabd = rutabd
    def __conctaneccion(self): 
        with open('Insumos\Drivers.yml', 'r', encoding='UTF-8') as file:
            config = yaml.safe_load(file) # archivo yml con algunas parametrizaciones en clave valor    

        #rutaarchivos = os.path.join(os.getcwd(),"Insumos")
        listaarchivos = os.listdir(self.ruta)
        compra = pd.DataFrame()
        for archivo in listaarchivos:
            rutaarchivo = os.path.join(self.ruta,archivo)
            data = pd.read_excel(rutaarchivo,sheet_name='AC - Compras')  
            data = data.rename(columns =config['Nombres_Columnas'])
            data = data.astype(config['Tipado']) # metodo para cambiar el tipado que esta en el archivo configuración yml.
            compra  = pd.concat([compra,data])
        return compra
    
    def __agrupacion(self):
        compra = self.__conctaneccion() 
        numerical_cols = compra.select_dtypes(include=['number']).columns
        sums = compra[numerical_cols].sum()
        total = pd.DataFrame({
        'Variable': numerical_cols,
        'Total': sums.values
        })          
        return total
    def diferenciascompra(self):
        real = self.__agrupacion()
        rutabd = os.path.join(self.rutabd,'BDSimuladorPpto.db')
        mi_conexión= sqlite3.connect(rutabd)
        query = """
                SELECT
	            SUM (PPTO_NETO_COP) AS PPTO_NETO_COP,
            	SUM(VENTA_NETA_COP)AS VENTA_NETA_COP,
                SUM(VENTA_NETA_ANT_COP) AS VENTA_NETA_ANT_COP,
	            SUM(VENTA_NETA_KG) AS VENTA_NETA_KG,                
	            SUM(VENTA_NETA_ANT_KG) AS VENTA_NETA_ANT_KG,	            
	            SUM(PPTO_NETO_KG) AS PPTO_NETO_KG,
                SUM(DCTOS_ACT) AS DCTOS_ACT,
                SUM(PPTO_DCTOS) AS PPTO_DCTOS 
                FROM Consolidado WHERE TIPO_VENTA = 'Compra'
            """
        consultaformato = pd.read_sql_query(query, mi_conexión)
        consultaformato_trans = consultaformato.T
        consultaformato_trans.reset_index(inplace=True)
        consultaformato_trans.columns =['Variable','Total']
        consultaformato_trans = pd.merge(consultaformato_trans,real, on = 'Variable',
                                         how='inner', suffixes=('_Calculado','_Real'))
        return consultaformato_trans