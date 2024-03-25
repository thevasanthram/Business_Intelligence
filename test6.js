const record = { id: "123123213001" };

const number_in_string = String(record["id"]);

record["id"] = Number(number_in_string.slice(0, -3));

console.log(record);
