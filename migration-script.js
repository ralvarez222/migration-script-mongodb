const MongoClient = require('mongodb').MongoClient
//const url = 'mongodb://localhost:27017/edx-course-db'
const customers = require('./m3-customer-data.json')
const customerAddress = require('./m3-customer-address-data.json')
const async = require('async')

let tasks = []
const lote = parseInt(process.argv[2])
// Connection URL
const url = 'mongodb://localhost:27017/edx_course_db'
// Use connect method to connect to the DB server
MongoClient.connect(url, (error, db) => {
  if (error) return process.exit(1)

  console.log('Conexion a BD')
  console.log(customers.length)

  var i
   
  if (lote == false) 
    {
      lote = 1000
    }

  for (i = 1; i <= customers.length; i++)
    {
      Object.assign(customers[i -1], customerAddress[i - 1])

      if (i % lote == 0 || i == customers.length) 
        {   
          let end = i - 1
          let start = 0
          start = i - lote 
                     
          tasks.push((done) => {
            console.log(`Processing ${start} to ${end}`)
            var dbo = db.db('edx_course_db')
            dbo.collection('customers').insert(customers.slice(start, end), (error, result) => {
                done(error, result)
            })
          })                  
        }       
    }
  //console.log(customers)
  console.log(tasks)
  console.log(`Launching ${tasks.length} paralell tasks`)
  const startTime = Date.now()
  async.parallel(tasks, (error, results) => {
      if (error) console.error(error)
      const endTime = Date.now()
      console.log(`Execution time: ${endTime - startTime}`)
      db.close
    })
  })