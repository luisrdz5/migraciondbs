### Migración de Bases de datos 

Este es un proyecto donde se generara 1 aplicativo en Python el cual se describe a continuación:


### MigracionDBS

Hara un analisis y comparacion de 2 BD's 


# Como crear un ambiente virtual 

se debe ejecutar el siguiente comando 

#python3 -m venv env
(env)#


# Ingresar al entorno virtual 

se debe ejecutar el siguiente comando 

#source env/bin/activate


# Instalar paquetes en el ambiente virtual 

(env)# pip3 install boto3


# Como generar archivo requeriments.txt 

Cuando ya se hallan instalado los paquetes necesarios se debera ejecutar el comando 

(env)# pip3 freeze > requeriments.txt 


# Como instalar el proyecto desde cero con las dependencias usando el archivo requeriments.txt 

Se debera ejecutar el comando: 


```sh
git clone git@github.com:luisrdz5/migraciondbs.git
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
python3 migracionDBS.py 
```



