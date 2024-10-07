/*
requires: npm install mongodb sequelize readline mysql2
*/

const readline = require('readline');
const { Sequelize, DataTypes} = require('sequelize');
const { MongoClient } = require ('mongodb');

const MYSQL_IP = "localhost";
const MYSQL_LOGIN = "root";
const MYSQL_PASSWORD = "root";
const DATABASE = "employees";

const sequelize = new Sequelize(DATABASE, MYSQL_LOGIN, MYSQL_PASSWORD, {
    host: MYSQL_IP,
    dialect: "mysql",
    //logging: false
});

const mongoClient = new MongoClient('mongodb://localhost:27017');

// Definir os modelos do Sequelize
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

const DeptManager = sequelize.define('dept_manager', {
    emp_no: { type: Sequelize.INTEGER, primaryKey: true },
    dept_no: Sequelize.STRING,
    from_date: Sequelize.DATE,
    to_date: Sequelize.DATE
}, { timestamps: false, tableName: 'dept_manager' });

async function employeeMap(empCollection){
    try {
        let batch_size = 1000;
        const departments = await Department.findAll();
        const managers = await DeptManager.findAll();
        const managersInfo = [];
        for (const manager of managers){
            const managerinfo = await Employee.findOne({where:{emp_no: manager.emp_no}});
            managersInfo.push(managerinfo)
        };

        offset = 0;
        let sobra = true;

        while (sobra){
            const employeeslimited = await Employee.findAll({ offset, limit: batch_size });
            if (employeeslimited.length === 0) {
                sobra = false;
                continue;
            }
            const employeeDocuments = [];
            for (const employee of employeeslimited){
                const salaries = await Salary.findAll({where:{emp_no: employee.emp_no}});
                const titles = await Title.findAll({where:{emp_no: employee.emp_no}});
                const empdepartment = await DeptEmp.findAll({where:{emp_no: employee.emp_no}});
                const manager = await DeptManager.findAll({where:{emp_no: employee.emp_no}});

                const employeeDocument = {
                    ...employee.get({plain: true}),
                    salaries: salaries.map(salary => salary.get({plain: true})),
                    titles: titles.map(title => title.get({plain: true})),
                    departments: empdepartment.map(department =>({
                        emp_no: department.toJSON().emp_no,
                        dept_no: department.toJSON().dept_no,
                        dept_name: departments.find(dept=> dept.dept_no === department.dept_no).dept_name,
                        from_date: department.toJSON().from_date,
                        to_date: department.toJSON().to_date,
                        managers: managers.filter(manager => {
                            return manager.dept_no === department.dept_no &&(manager.from_date <= department.to_date || manager.to_date >= department.from_date)
                        }).map(manager =>({
                            emp_no: manager.toJSON().emp_no,
                            first_name: managersInfo.find(managerinfo => managerinfo.emp_no === manager.emp_no).first_name,
                            from_date: manager.toJSON().from_date,
                            to_date: manager.toJSON().to_date
                        }))
                    })),
                    manager_at: manager.map(manager =>({
                        dept_no: manager.toJSON().dept_no,
                        dept_name: departments.find(dept=> dept.dept_no === manager.dept_no).dept_name,
                        from_date: manager.toJSON().from_date,
                        to_date: manager.toJSON().to_date
                    }))
                };
                employeeDocuments.push(employeeDocument);
            }
            await empCollection.insertMany(employeeDocuments);
            offset += batch_size;
        }
    }catch (error){console.log('erro ao mapear employees', error)};
}

async function criaIndex(){
    try{
        await sequelize.query('create index idx_emp_no ON employees.salaries (emp_no); create index idx_emp_no ON employees.employees (emp_no); create index idx_emp_no ON employees.titles (emp_no); create index idx_emp_no ON employees.dept_emp (emp_no)')
        console.log('criados indices com sucesso');}catch(error){};
}
async function destroiIndex(){
    try{
        await sequelize.query('drop index idx_emp_no ON employees.salaries; drop index idx_emp_no ON employees.employees; drop index idx_emp_no ON employees.titles; drop index idx_emp_no ON employees.dept_emp;')
        console.log('removido indices com sucesso');}catch(error){};
}

async function main() {

    let mdb;
    const collectionName = 'employees';
    criaIndex();
    try{
        await mongoClient.connect();
        console.log('conectado ao MongoDB');
        mdb = mongoClient.db(collectionName);
        const empCollectionE = mdb.listCollections({name:collectionName});
        if (empCollectionE) {await mdb.collection(collectionName).drop()};
        const empCollection = await mdb.createCollection(collectionName);
        await employeeMap(empCollection);
        console.log('sucesso no upload');
    } catch (error) {console.error('Erro ao conectar ao MongoDB', error)};
    destroiIndex();
}

main();
