# Lambda Prueba

Este proyecto proporciona una API para realizar operaciones CRUD básicas sobre usuarios. La API permite recuperar, agregar, actualizar y eliminar usuarios en la base de datos. Los métodos disponibles son GET, POST, PUT y DELETE.

## Método GET

**URL:**  
`https://708n64aili.execute-api.us-east-1.amazonaws.com/test`

**Ejemplo de respuesta:**
```json
[
    {
        "id": 1,
        "nombre": "Yasser",
        "email": "yasser@gmail.com"
    },
    {
        "id": 2,
        "nombre": "Miguel Edit",
        "email": "miguel@gmail.com"
    }
]
```

# Method POST

**Cuerpo de la solicitud:**

```json
{
    "id" : 2,
    "nombre" : "Miguel Edit",
    "email" : "miguel@gmail.com"

}
```
**Ejemplo de respuesta:**

```json
{
    "message": "User inserted successfully"
}
```
# Method PUT

**Cuerpo de la solicitud:**

```json
{
    "id" : 3,
    "nombre" : "Miguel",
    "email" : "miguel@gmail.com"

}
```

**Ejemplo de respuesta:**

```json
{
    "message": "User updated successfully",
    "affectedRows": 1
}
```

# Method DELETE

**Cuerpo de la solicitud:**

```json
{
    "id" : 3
}
```

**Ejemplo de respuesta:**

```json
{
    "message": "User deleted successfully",
    "affectedRows": 1
}
```
