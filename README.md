# Proyecto-kafka
# Transaccional consumer
el consumer "isolation.level","read_committed" significa que el producer debio dar commit en caso contrario no lee ningun mensaje.
en cambio un consumer que no tiene  "isolation.level","read_committed" leera todos los mensajes dejados por el producer hasta que dio excepcion

# group
se generan 5 hilos de consumidor, pero como estan en un grupo consumen diferentes particiones, se consumen en paralelo segun va llegando el mensaje a la particion. cadda hilo solo lee sus particion asignado.

pero el consumer que no esta en el grupo si debe leer  todos los mensajes.

# Orden mensajes
se debe poner el mismo key para que vaya a una sola particion y este ordenada

# Assign seek

