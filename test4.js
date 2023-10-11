const timezoneOffsetHours = 5; // 5 hours ahead of UTC
const timezoneOffsetMinutes = 30; // 30 minutes ahead of UTC

let createdBeforeTime = new Date();

createdBeforeTime.setHours(
  createdBeforeTime.getHours() - 1 + timezoneOffsetHours
);
createdBeforeTime.setMinutes(
  createdBeforeTime.getMinutes() + timezoneOffsetMinutes
);
console.log("createdBeforeTime: ", createdBeforeTime);
