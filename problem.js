// let num = -96;
// let highest_power = 0;

// let num_cpy = num;

// while (num_cpy % 2 == 0) {
//   num_cpy /= 2;
//   highest_power++;
// }

// if (num < 0) {
//   highest_power = highest_power * -1;
// }

// console.log("Entered Number: ", num);
// console.log("Highest power of 2 is ", highest_power);

// =====================================================

const command_string = "M 5 D 10 M 10 R 5";
const commands = command_string.split(" ");

let current_direction = "R";

let x_coordinate = 0;
let y_coordinate = 0;

let intersection_count = 0;

function find_new_position(direction, distance) {
  current_direction = direction;
  switch (direction) {
    case "M": {
      x_coordinate = x_coordinate + parseInt(distance);
      break;
    }
    case "R": {
      x_coordinate = x_coordinate + parseInt(distance);
      break;
    }
    case "L": {
      x_coordinate = x_coordinate - parseInt(distance);
      break;
    }
    case "U": {
      y_coordinate = y_coordinate + parseInt(distance);
      break;
    }
    case "D": {
      y_coordinate = y_coordinate - parseInt(distance);
      break;
    }
  }
}

for (let i = 0; i < commands.length; i = i + 2) {
  //   console.log(commands[i]);
  //   console.log(commands[i + 1]);

  if (commands[i] == "M") {
    find_new_position(current_direction, commands[i + 1]);
  } else {
    find_new_position(commands[i], commands[i + 1]);
  }
}

console.log(
  `The final position of Robot is (${x_coordinate} , ${y_coordinate}) `
);

console.log(
  `The path has intersected for ${intersection_count} times of its own.`
);
