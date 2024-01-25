async function demo() {
  await new Promise((resolve, reject) => {
    setTimeout(() => {
      console.log("hiii");
      resolve();
    }, 3000);
  });
}

async function main() {
  await demo();
  console.log("last line");
}

main();
