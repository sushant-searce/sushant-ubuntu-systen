// const http = require('http');
// const oracledb = require('oracledb');
// let error;
// let user;
 
// oracledb.getConnection(
//     {
//       user: Admin, 
//       password: password,
//       connectString: 'sushant-oracle-db.cih0jkvggyaj.us-east-2.rds.amazonaws.com/DATABASE:1521'
//     }, 
//     function(err, connection) {
//       if (err) {error = err; return;}
      
//       connection.execute('select * from entries', [], function(err, result) {
//         if (err) {error = err; return;} 
//         user = result.rows[0][0];
//         error = null; 
//         connection.close(function(err) {
//           if (err) {console.log(err);}
//         });
//       })
//     }
// );
 
// http.createServer(function(request, response) {
//   response.writeHead(200, {'Content-Type': 'text/plain'});
 
//   if (error === null) {
//     response.end('Connection test succeeded. You connected to Exadata Express as ' + user + '!');
//   } else if (error instanceof Error) {
//     response.write('Connection test failed. Check the settings and redeploy app!\n');
//     response.end(error.message);
//   } else {
//     response.end('Connection test pending. Refresh after a few seconds...');
//   }
// }).listen(process.env.PORT);

// ================================================================
var oracledb = require('oracledb');  
  
oracledb.getConnection({  
     user: "admin",  
     password: "password",  
     connectString: "sushant-oracle-db.cih0jkvggyaj.us-east-2.rds.amazonaws.com/DATABASE:1521"  
}, function(err, connection) {  
     if (err) {  
          console.error(err.message);  
          return;  
     }  
     connection.execute( "select * from entries",  
     [],  
     function(err, result) {  
          if (err) {  
               console.error(err.message);  
               doRelease(connection);  
               return;  
          }  
          console.log(result.metaData);  
          console.log(result.rows);  
          doRelease(connection);  
     });  
});  
  
function doRelease(connection) {  
     connection.release(  
          function(err) {  
               if (err) {console.error(err.message);}  
          }  
     );  
}  