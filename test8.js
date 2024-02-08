const readline = require("readline");

function calculateFinalPositionAndIntersects(commandString) {
  let x = 0;
  let y = 0;
  let direction = "right";
  let visited = new Set();
  let intersects = 0;

  const move = (distance) => {
    for (let i = 0; i < distance; i++) {
      if (direction === "right") {
        x++;
      } else if (direction === "left") {
        x--;
      } else if (direction === "up") {
        y++;
      } else if (direction === "down") {
        y--;
      }

      const currentPosition = `${x},${y}`;
      if (visited.has(currentPosition)) {
        intersects++;
      } else {
        visited.add(currentPosition);
      }
    }
  };

  const turnRight = () => {
    if (direction === "right") {
      direction = "down";
    } else if (direction === "down") {
      direction = "left";
    } else if (direction === "left") {
      direction = "up";
    } else if (direction === "up") {
      direction = "right";
    }
  };

  const turnLeft = () => {
    if (direction === "right") {
      direction = "up";
    } else if (direction === "up") {
      direction = "left";
    } else if (direction === "left") {
      direction = "down";
    } else if (direction === "down") {
      direction = "right";
    }
  };

  const commands = commandString.split(" ");

  for (let i = 0; i < commands.length; i += 2) {
    const action = commands[i];
    const value = parseInt(commands[i + 1]);

    if (action === "M") {
      move(value);
    } else if (action === "R") {
      turnRight();
    } else if (action === "L") {
      turnLeft();
    }
  }

  const finalPosition = `${x},${y}`;
  const result = intersects === 0 ? finalPosition : "INF";

  return { finalPosition: result, intersects };
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

rl.question("Enter command string: ", (commandString) => {
  const { finalPosition, intersects } =
    calculateFinalPositionAndIntersects(commandString);
  console.log(`Final position: ${finalPosition}`);
  console.log(`Number of intersects: ${intersects}`);
  rl.close();
});
