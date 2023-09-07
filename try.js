// Sample object
const person = {
  firstName: "John",
  lastName: "Doe",
  age: 30,
};

// Rename the "firstName" property to "first_name" without changing property order
person.first_name = person.firstName;
delete person.firstName;

console.log(person);
