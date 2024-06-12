import os
import Modulos.concatenar as concat
import warnings
import Modulos.validacioncompra as comp
warnings.filterwarnings('ignore')
ruta = os.path.join(os.getcwd(),"Bo")
rutabd = os.path.join(os.getcwd(),"Salidas")
def run():
    #ruta = os.path.join(os.getcwd(),"Bo")
    rutatrans = os.path.join(os.getcwd(),"Insumos")
    listaArchivos = os.listdir(ruta)
    transfor = os.path.join(rutatrans,'Transformados_CN.xlsx')
    for archivo in listaArchivos:
        datos = os.path.join(ruta,archivo)        
        calculo = concat.concatnerinfo(datos,transfor)
        #rutacalle = os.path.join(os.getcwd(),'AC - Calle.xlsx')
        #rutacompra = os.path.join(os.getcwd(),'AC - Compras.xlsx')
        #calculo.ponderacioncompr(rutacalle,rutacompra)
        calculo.Concatenerinfo()
    
def corrercompra():
    objetocompra = comp.compra(ruta,rutabd)
    sumacompra = objetocompra.diferenciascompra()
    return sumacompra

if __name__ == '__main__':
    run()
    comparativa = corrercompra()
    comparativa.to_excel("comparativa.xlsx",index=False)
    
    
    
    
    
    













