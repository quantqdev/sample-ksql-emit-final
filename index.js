(async () => {
    const {setTimeout} = require('timers/promises');
    const { Kafka } = require("kafkajs");
    const chalk = await import("chalk").then(m=>m.default);
  
    const kafka = new Kafka({
      clientId: "my-app",
      brokers: ["localhost:9092"],
    });


    const consumer = kafka.consumer({ groupId: 'my-group' })
    await consumer.connect()
    await consumer.subscribe({ topics: ['user-record-in-window'] })

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            const obj = JSON.parse(message.value.toString())
            console.log(chalk.green(`OUT: ${obj.WINDOW_START} - ${obj.WINDOW_END}: ${obj.USER_ID} | ${obj.PROP1} | ${obj.PROP2}`))
        },
    })


  
    const producer = kafka.producer();
    await producer.connect();


    const send = async(i)=>{
      const userId = Math.floor(Math.random() * 5) + 1;

      const value = {
        timestamp: new Date().toISOString(),
        properties: {
          user_id: `user_${userId}`,
          prop1: `prop1_${userId}_${i}`,
          prop2: `prop2_${userId}_${i}`,
        },
      };

      console.log(chalk.yellow(`IN : ${value.timestamp}                         : ${value.properties.user_id} | ${value.properties.prop1} | ${value.properties.prop2}`))
  
      await producer.send({
        topic: "user-record",
        messages: [{ value: JSON.stringify(value) }],
      });
    }
  
    for(let i = 0; i < 5; i++){
        for(let j = 0; j < 10; j++){
            await send(i*5+j);
            await setTimeout(Math.random()*3000)
        }
        const waitTime = Math.random()*20000
        console.log(chalk.magenta(`WAIT: ${(waitTime/1000).toFixed(2)}s`))
        await setTimeout(waitTime)
    }

    process.exit();

  })();
