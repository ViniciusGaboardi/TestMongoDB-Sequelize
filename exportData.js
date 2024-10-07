const readline = require('readline');
const { Sequelize, DataTypes } = require('sequelize');
const { MongoClient } = require('mongodb');

const MYSQL_IP = "localhost";
const MYSQL_LOGIN = "root";
const MYSQL_PASSWORD = "root";
const DATABASE = "employees";

const sequelize = new Sequelize(DATABASE, MYSQL_LOGIN, MYSQL_PASSWORD, {
    host: MYSQL_IP,
    dialect: "mysql",
    logging: false
});

const mongoUrl = 'mongodb://localhost:27017';
const mongoClient = new MongoClient(mongoUrl);

const Employee = sequelize.define('employee', {
    emp_no: { type: Sequelize.INTEGER, primaryKey: true },
    birth_date: Sequelize.DATE,
    first_name: Sequelize.STRING,
    last_name: Sequelize.STRING,
    gender: Sequelize.ENUM('M', 'F'),
    hire_date: Sequelize.DATE
}, { timestamps: false });

const Salary = sequelize.define('salary', {
    emp_no: { type: Sequelize.INTEGER, primaryKey: true },
    salary: Sequelize.INTEGER,
    from_date: Sequelize.DATE,
    to_date: Sequelize.DATE
}, { timestamps: false });

const Title = sequelize.define('title', {
    emp_no: { type: Sequelize.INTEGER, primaryKey: true },
    title: Sequelize.STRING,
    from_date: Sequelize.DATE,
    to_date: Sequelize.DATE
}, { timestamps: false });

const Department = sequelize.define('department', {
    dept_no: { type: Sequelize.STRING, primaryKey: true },
    dept_name: Sequelize.STRING
}, { timestamps: false });

const DeptEmp = sequelize.define('dept_emp', {
    emp_no: { type: Sequelize.INTEGER, primaryKey: true },
    dept_no: Sequelize.STRING,
    from_date: Sequelize.DATE,
    to_date: Sequelize.DATE
}, { timestamps: false, tableName: 'dept_emp' });

DeptEmp.belongsTo(Department, { foreignKey: 'dept_no' });

const DeptManager = sequelize.define('dept_manager', {
    emp_no: { type: Sequelize.INTEGER, primaryKey: true },
    dept_no: Sequelize.STRING,
    from_date: Sequelize.DATE,
    to_date: Sequelize.DATE
}, { timestamps: false, tableName: 'dept_manager' });

async function createIndex() {
    await mongoClient.connect();
    const db = mongoClient.db(DATABASE);
    const collection = db.collection('employees');
    await collection.createIndex({ emp_no: 1 }, { unique: true });
    await mongoClient.close();
    console.log('Unique index on emp_no created successfully.');
}

async function migrateData() {
    try {
        await sequelize.authenticate();
        console.log('Connection to MySQL has been established successfully.');
        await mongoClient.connect();
        console.log('Connected to MongoDB successfully.');
        const db = mongoClient.db(DATABASE);
        const collection = db.collection('employees');

        const pageSize = 10000; // Tamanho do lote

        let offset = 0;
        let shouldContinue = true;

        while (shouldContinue) {
            const employees = await Employee.findAll({
                offset: offset,
                limit: pageSize
            });

            if (employees.length === 0) {
                shouldContinue = false;
                break;
            }

            const employeeDocs = [];

            for (const emp of employees) {
                const emp_no = emp.emp_no;
                console.log(`Migrating employee: ${emp_no}`);

                const salaries = await Salary.findAll({ where: { emp_no } });
                const titles = await Title.findAll({ where: { emp_no } });
                const deptEmp = await DeptEmp.findAll({ where: { emp_no }, include: Department });
                const deptManager = await DeptManager.findAll({ where: { emp_no } });

                const employeeDoc = {
                    emp_no: emp_no,
                    birth_date: emp.birth_date,
                    first_name: emp.first_name,
                    last_name: emp.last_name,
                    gender: emp.gender,
                    hire_date: emp.hire_date,
                    salaries: salaries.map(s => ({
                        salary: s.salary,
                        from_date: s.from_date,
                        to_date: s.to_date
                    })),
                    titles: titles.map(t => ({
                        title: t.title,
                        from_date: t.from_date,
                        to_date: t.to_date
                    })),
                    departments: deptEmp.map(d => ({
                        dept_no: d.dept_no,
                        department_name: d.department ? d.department.dept_name : null, // Verifica se o departamento está presente
                        from_date: d.from_date,
                        to_date: d.to_date
                    })),
                    manager_departments: deptManager.map(dm => ({
                        dept_no: dm.dept_no,
                        from_date: dm.from_date,
                        to_date: dm.to_date
                    }))
                };

                employeeDocs.push(employeeDoc);
            }

            if (employeeDocs.length > 0) {
                try {
                    await collection.insertMany(employeeDocs, { ordered: false });
                } catch (error) {
                    if (error.code === 11000) {
                        console.log('Duplicate key error, skipping duplicates.');
                    } else {
                        throw error;
                    }
                }
            }

            offset += pageSize;
        }

        console.log('Data migration completed successfully.');
    } catch (error) {
        console.error('An error occurred during data migration:', error);
    } finally {
        await sequelize.close();
        await mongoClient.close();
    }
}

async function main() {
    await createIndex();
    await migrateData();
}

main();
