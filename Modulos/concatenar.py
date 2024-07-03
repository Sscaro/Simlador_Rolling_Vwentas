import pandas as pd
import logging
import yaml
import unicodedata
import numpy as np
import sqlite3

def remove_special_characters(text):
    '''
    Metodo para remover caracartres especiales
    ARG: Text: palabra que se quiere organizar
    '''
    # Normalizar el texto para eliminar caracteres especiales
    nfkd_form = unicodedata.normalize('NFKD', text)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('ASCII')
    # Eliminar caracteres especiales adicionales
    cleaned_text = ''.join(char for char in only_ascii if char.isalnum() or char.isspace())
    return cleaned_text

def reemplazarvalores(datos, column, diccionario):
    '''
    funcion para reemplazar valores dado un archivo de configuraciones
    ARGS: datos: data frame completo
        column: columna que se desea hacer reemplazo
        dicionario: diccionario de claves y valores que se desean hacer los reemplazos.
    '''
    datos[column] = datos[column].map(diccionario).fillna(datos[column])
    return datos


def totalizar(data, columnas,calculo:int,nombrecol,colmultiplicar):
    """
    Funcion para sacar total de una agrupacion especifica
    ARG: data = data frame completo
        columnas = columnas por la que se quieren agrupar   
        calculo = columna númerica por lo que se quiere tootalizar
        nombrecol = nombre de la nueva columna
        colmultiplicar = columna por la que se debe multiplicar el ponderado
    """
    total = data.groupby(columnas).agg(TOTAL=(calculo,'sum')).reset_index()
    resultado = pd.merge(data,total, on = columnas, how = 'inner')
    #nombrecol = 'POND_'+nombrecol
    resultado[nombrecol] = np.where(resultado['TOTAL'] != 0, resultado[calculo] / resultado['TOTAL'], 0)
    resultado[nombrecol] = resultado[nombrecol]*resultado[colmultiplicar]
    del resultado['TOTAL']
    del resultado[calculo]  
    del resultado[colmultiplicar]
    return resultado

def agrupar_por_categoricas(data,descuentos = False):
    '''
    funcion para agrupar todo un data frame por todas sus variables categoricas y sumar
    sus numericas
    ARG: data: Data Frame
    descuentos : Si se desean excluir los descuentos.
    '''
 
    # Agrupar por las columnas categóricas y sumar las columnas numéricas
    
    if descuentos == True:
        coldescuentos = ['PPTO_NETO_COP','VENTA_NETA_COP','VENTA_NETA_KG',
                         'VENTA_NETA_ANT_COP','VENTA_NETA_ANT_KG','PPTO_NETO_KG']
        for col in data.columns:
            if col in coldescuentos:
                del data[col]
        data['TIPOLOGIA'] = 'Agente Comercial'
      
    else:
        coldescuentos=['DCTOS_ACT','PPTO_DCTOS']
        for col in data.columns:
            if col in coldescuentos:
                del data[col] 
       
    cols_categoricas = data.select_dtypes(include=['object', 'category']).columns.tolist()
    # Agrupar por las columnas categóricas y sumar las columnas numéricas
    cols_numericas = data.select_dtypes(include=['number']).columns.tolist()
    grouped_df = data.groupby(cols_categoricas)[cols_numericas].sum().reset_index()
    return grouped_df


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',    
    handlers=[
#        logging.FileHandler('log_ejecuciones.txt'),
        logging.StreamHandler()
    ]
)

def validacionCompra(dataframe):
    """
    funcion para calular la suma de las variables numericas
    ARG: dataframe: marco de datos
    """
    numerical_cols = dataframe.select_dtypes(include=['number']).columns
    sums = dataframe[numerical_cols].sum()
    agrupado = pd.DataFrame({
    'Variables': numerical_cols,
    'Total': sums.values
     })
    return agrupado 

class concatnerinfo:
    '''
    Clase para concetanar diferentes archivos
    '''
    def __init__(self,ruta,rutatrans):
        self.ruta = ruta
        self.rutatrans = rutatrans      
        self.diccionario_dataframes = {} # diccionario para alamencar data frames

    def __procesar_excel(self):
        """
        Metodo para leer los difernetes archivos excel
        luego lee cada una de las hojas, concatenada los diferentes dataframes y devuelve un diccionar
        con los nombres del df y sus respectivos datos.
        """
        try:
            logging.info("Comienza lectura de archivos")
            logging.info("Este proceso puede ser tardado...")          
            hojas = pd.read_excel(self.ruta, sheet_name=None)
            for nombre_hoja, df in hojas.items():
                if nombre_hoja in self.diccionario_dataframes:
                        
                    for col in df.select_dtypes(include=[object]).columns:
                        df[col] = df[col].apply(lambda x: remove_special_characters(str(x)))

                    self.diccionario_dataframes[nombre_hoja] = pd.concat([self.diccionario_dataframes[nombre_hoja], df])
                else:
                    for col in df.select_dtypes(include=[object]).columns:
                        df[col] = df[col].apply(lambda x: remove_special_characters(str(x)))
                    self.diccionario_dataframes[nombre_hoja] = df
            logging.info(f"Lectura de archivos exitosa ,{self.ruta}!!")
        
        except:
            logging.info("""Algo fallo, revisa los archivos que cargaste...
                        Recuerda que los archivos deben tener exactamente la misma estrcutura""")
    
    def __aplicartransformados(self,dataframe):
        '''
        Metodo  para leer los transoformados y manejo de tildes y caractares especiales.
        '''       
        trans = pd.read_excel(self.rutatrans)
        for col in trans.select_dtypes(include=[object]).columns:
            trans[col] = trans[col].apply(lambda x: remove_special_characters(str(x)))
        trans = pd.merge(dataframe,trans, on = 'TIPOLOGIA', how = 'left')    
        return trans
    
    def OranizarDataframes(self):
        self.__procesar_excel()
       
        with open('Insumos\Drivers.yml', 'r', encoding='UTF-8') as file:
            config = yaml.safe_load(file) # archivo yml con algunas parametrizaciones en clave valor
        with open('Insumos\Reemplazos.yml', 'r', encoding='UTF-8') as file:
            config2 = yaml.safe_load(file) # archivo yml con algunas parametrizaciones en clave valor
       # with open('Insumos\Reemplazos.yml', 'r', encoding='UTF-8') as file:
       #     config3 = yaml.safe_load(file) # archivo yml con algunas parametrizaciones en clave valor

        #driver_trans = self.__leertransformados()    
        for nombre_df, df in self.diccionario_dataframes.items():            
            df = df.rename(columns =config['Nombres_Columnas'])
            #var_numericas = df.select_dtypes(include=[np.number]).columns
            #df[var_numericas] = df[var_numericas].fillna(0)
            df = df.astype(config['Tipado']) # metodo para cambiar el tipado que esta en el archivo configuración yml.
                        
            if nombre_df == 'Sin GC y AC' or nombre_df=='Digital' or nombre_df=='GC' :
                #df = pd.merge(df,driver_trans, on = 'TIPOLOGIA', how='left')
                df['TIPO_VENTA'] = 'Compañia'
                #df.to_excel(f'{nombre_df}.xlsx',index=False)     
        
            elif nombre_df=='AC - Compras':
                del df['TIPOLOGIA']                
                for col, par in config2.items():
                    df = reemplazarvalores(df,col,par)

                #df = agrupar_por_categoricas(df)
                df['TIPO_VENTA']  = 'Compra' 
                #df.to_excel(f'{nombre_df}.xlsx',index=False)              

            elif nombre_df=='AC - Calle':
                #df = pd.merge(df,driver_trans, on = 'TIPOLOGIA', how='left')
                df['TIPO_VENTA']  = 'Calle'       
                del df['DCTOS_ACT']
                del df['PPTO_DCTOS']
                #df.to_excel(f'{nombre_df}.xlsx',index=False)

            self.diccionario_dataframes[nombre_df] = df
            columNum = df.select_dtypes(include=[np.number]).columns
            df[columNum] = df[columNum].fillna(0)
            #df.to_excel(f'{nombre_df}.xlsx',index=False)        
            logging.info(f'Hoja leida {nombre_df}')
        
    #def  ponderacioncompr(self,rutacalle,rutacompra):
    def  ponderacioncompr(self):
        '''
        Metodo para ponderar la compra con la tipologia de la venta a la calle.
        '''

        self.OranizarDataframes()
        compra =  self.diccionario_dataframes['AC - Compras']
        calle = self.diccionario_dataframes['AC - Calle']
        #compra = pd.read_excel(rutacompra,dtype={'NIF':str,'COD_CLIENTE':str})
        #calle = pd.read_excel(rutacalle,dtype={'NIF':str,'COD_CLIENTE':str})
        compra_dctos = compra.copy()
        compra = agrupar_por_categoricas(compra)        
        compra_dctos = agrupar_por_categoricas(compra_dctos, descuentos=True)        
        nuevacompra = calle.copy()
        borrar_col = ['NIF','AGRUPA_CLIENTES','FORMATO',
                      'NOM_CLIENTE','TIPO_VENTA','SECTOR_CLAVE',
                      'CATEGORIA_CLAVE','SUB_CATEGORIA_CLAVE','LINEA_CLAVE',
                      'MARCA_CLAVE']
        
        for i in borrar_col:
            del nuevacompra[i]

        columnas_merge = ['OFICINA_VENTAS','COD_CLIENTE','SECTOR','CATEGORIA',
                          'SUB_CATEGORIA','LINEA','MARCA','MES'] 
        nuevacompra = pd.merge(nuevacompra,compra, on = columnas_merge,how='inner',suffixes=('_CALLE','_COMPRA'))  
        nuevacompra = totalizar(nuevacompra,columnas_merge,'PPTO_NETO_COP_CALLE','PPTO_NETO_COP','PPTO_NETO_COP_COMPRA')
        nuevacompra = totalizar(nuevacompra,columnas_merge,'VENTA_NETA_COP_CALLE','VENTA_NETA_COP','VENTA_NETA_COP_COMPRA')
        nuevacompra = totalizar(nuevacompra,columnas_merge,'VENTA_NETA_KG_CALLE','VENTA_NETA_KG','VENTA_NETA_KG_COMPRA')
        nuevacompra = totalizar(nuevacompra,columnas_merge,'VENTA_NETA_ANT_COP_CALLE','VENTA_NETA_ANT_COP','VENTA_NETA_ANT_COP_COMPRA')
        nuevacompra = totalizar(nuevacompra,columnas_merge,'VENTA_NETA_ANT_KG_CALLE','VENTA_NETA_ANT_KG','VENTA_NETA_ANT_KG_COMPRA')
        nuevacompra = totalizar(nuevacompra,columnas_merge,'PPTO_NETO_KG_CALLE','PPTO_NETO_KG','PPTO_NETO_KG_COMPRA')
                
        nuevacompra = pd.concat([nuevacompra,compra_dctos])
        nuevacompra[nuevacompra.select_dtypes(include=[np.number]).columns] = nuevacompra.select_dtypes(include=[np.number]).fillna(0)
        nuevacompra = self.__aplicartransformados(nuevacompra)
        nuevacompra['CANAL_DCTOS'] = 'Tradicional'
        nuevacompra['SUB_CANAL_DCTOS'] = 'Agente Comercial'
        nuevacompra['TIPOLOGIA_DCTOS'] = nuevacompra['TIPOLOGIA'] 
        #nuevacompra.to_excel('calle_pondv.xlsx',index=False)
        
        return nuevacompra
    
    def Concatenerinfo(self):
       '''
       Metodo ejecución ponderación de compra a calle, y consolidación 
       de en la base de datos
       '''
       
       compraponderada = self.ponderacioncompr()     
       conexion = sqlite3.connect('Salidas\BDSimuladorPpto.db')      
       logging.info("""Comienza proceso de anexo a la base de datos...""")
       for nombre_df,df in self.diccionario_dataframes.items():
            if nombre_df == 'Sin GC y AC' or  nombre_df=='Digital':
                df = self.__aplicartransformados(df)
                df['CANAL_DCTOS'] = df['CANAL']
                df['SUB_CANAL_DCTOS'] = df['SUB_CANAL']
                df['TIPOLOGIA_DCTOS'] = df['TIPOLOGIA']
                df.to_sql("Consolidado",conexion, if_exists="append")

            elif nombre_df == 'AC - Calle':
                df = self.__aplicartransformados(df)
                df['CANAL_DCTOS'] = df['CANAL']
                df['SUB_CANAL_DCTOS'] = df['SUB_CANAL']
                df['TIPOLOGIA_DCTOS'] = df['TIPOLOGIA']
                df['DCTOS_ACT'] = 0
                df['PPTO_DCTOS'] = 0
                df.to_sql("Consolidado",conexion, if_exists="append")

            elif nombre_df=='GC':
                df = self.__aplicartransformados(df)
                df['CANAL_DCTOS'] = df['CANAL']
                df['SUB_CANAL_DCTOS'] = df['FORMATO']
                df['TIPOLOGIA_DCTOS'] = df['TIPOLOGIA'] 
                df.to_sql("Consolidado",conexion, if_exists="append")
            elif nombre_df=='AC - Compras':
                 compraponderada.to_sql("Consolidado",conexion, if_exists="append")
       conexion.close()           
            
        
       logging.info(f"Igesta de archivo {self.ruta}  extitosa!!")

            
