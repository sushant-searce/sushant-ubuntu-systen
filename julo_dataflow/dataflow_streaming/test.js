function transform(line) {
var values = line.split(',');

var obj = new Object();
obj.Name = values[0];
obj.Age = values[1];
obj.Job = values[2];
obj.Marks = values[3];
obj.Aggr = values[4];


var jsonString = JSON.stringify(obj);

return jsonString;
}
