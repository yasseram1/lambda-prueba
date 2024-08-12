import mysql from 'mysql';

const con = mysql.createConnection({
    host: 'xxxx',
    user: 'xxxx',
    port: '3306',
    password: 'xxxx',
    database: 'test'
});

const TABLE_TEST = 'user';

export const handler = async (event, context) => {

    context.callbackWaitsForEmptyEventLoop = false;

    const query = (sql, values) => new Promise((resolve, reject) => {
        con.query(sql, values, (err, res) => {
            if (err) {
                return reject(err);
            }
            resolve(res);
        });
    });

    if (event.httpMethod === 'POST') {
        const { id, nombre, email } = JSON.parse(event.body);
        const sql = `INSERT INTO ${TABLE_TEST} (id, nombre, email) VALUES (?, ?, ?);`;
        try {
            await query(sql, [id, nombre, email]);
            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'User inserted successfully' }),
            }
        } catch (error) {
            return {
                statusCode: 500,
                body: JSON.stringify({ error: 'Error inserting user', details: error.message }),
            }
        }
    }

    if (event.httpMethod === 'GET') {
        const sql = `SELECT * FROM ${TABLE_TEST};`;

        try {
            const results = await query(sql);

            return {
                statusCode: 200,
                body: JSON.stringify(results, event),
            };
        } catch (error) {
            return {
                statusCode: 500,
                body: JSON.stringify({ error: 'Error fetching users', details: error.message }),
            };
        }
    }

    if (event.httpMethod === 'PUT') {
        const { id, nombre, email } = JSON.parse(event.body);
        const sql = `UPDATE ${TABLE_TEST} SET nombre = ?, email = ? WHERE id = ?;`;

        try {
            const results = await query(sql, [nombre, email, id]);

            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'User updated successfully', affectedRows: results.affectedRows }),
            };
        } catch (error) {
            return {
                statusCode: 500,
                body: JSON.stringify({ error: 'Error updating user', details: error.message }),
            };
        }
    }
    
    if (event.httpMethod === 'DELETE') {
        const { id, nombre, email } = JSON.parse(event.body);
        const sql = `DELETE FROM ${TABLE_TEST} WHERE id = ?;`;

        try {
            const results = await query(sql);

            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'User deleted successfully', affectedRows: results.affectedRows }),
            };
        } catch (error) {
            return {
                statusCode: 500,
                body: JSON.stringify({ error: 'Error deleting user', details: error.message }),
            };
        }
    }


    return {
        statusCode: 405,
        body: JSON.stringify({ message: 'Method Not Allowed' }),
    };
}
