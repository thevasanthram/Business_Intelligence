const flatten_object = require("./modules/new_flatten_object");
const extract_matching_values = require("./modules/extract_matching_values");

const a = {
  one: "1",
  two: "2",
  three: 2,
  four: 2,
};

const b = {
  one: "1",
  two: "2",
  three: {
    three_one: "3.1",
    three_two: "3.2",
  },
  four: {
    four_one: "3.1",
    four_two: "3.2",
  },
};

const a_flatten = flatten_object(a);
const b_flatten = flatten_object(b);

console.log("a_flatten: ", a_flatten);
console.log("b_flatten: ", b_flatten);

const final_output = extract_matching_values(b_flatten, a_flatten);

// console.log("final_output: ", final_output);
