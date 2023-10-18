function auto_update() {
  console.log("enteringg");
  // Get the current date and time
  const previous_batch_time = new Date("2023-10-18T13:00:00.000Z");
  const previous_batch_hour = previous_batch_time.getHours();

  // Calculate the next hour
  const previous_batch_next_hour = (previous_batch_hour + 1) % 24;

  const now = new Date();
  const currentHour = now.getHours();

  // Check if it's the next hour
  if (currentHour != previous_batch_next_hour) {
    // Schedule the next call after an hour
    const timeUntilNextHour = (60 - now.getMinutes()) * 60 * 1000; // Calculate milliseconds until the next hour
    console.log("timer funtion entering", timeUntilNextHour);
    setTimeout(auto_update, timeUntilNextHour);
  } else {
    console.log("next batch initiated");
  }
}

// auto_update();
setTimeout(auto_update, 30000);