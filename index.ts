import * as aws from "aws-sdk";

const tableName = "dev-test-modn-2025-lottery-ModNPriorityQueue-table";

export async function dequeue(limit = 100): Promise<any[]> {
  const param = {
    TableName: tableName,
    IndexName: "modn-queue-index",
    ExpressionAttributeNames: {
      "#inFlight": "inFlight",
    },
    ExpressionAttributeValues: {
      ":yes": 1,
    },
    KeyConditionExpression: "#inFlight = :yes",
    ScanIndexForward: true,
    Limit: limit,
  };
  // console.log("dequeue param", param);

  const dynamo = new aws.DynamoDB.DocumentClient({
    region: "ap-northeast-1",
    apiVersion: "2012-08-10",
    signatureVersion: "v4",
    maxRetries: 3,
    retryDelayOptions: {
      base: 100,
    },
    // httpOptions: {
    //   timeout: 5000, // Change to 5s to avoid slow fail
    // },
  });

  try {
    const result = await dynamo.query(param).promise();

    if (result.Items!.length) {
      // console.log("🎉 Dequeue success", result.Items!.length);
    }

    return result.Items;
  } catch (e: any) {
    console.error(e);
    throw e;
  }
}

async function queryDynamoDB(): Promise<{
  isSuccess: boolean;
  timeInMs: number;
}> {
  const startTs = new Date().getTime();
  try {
    // Execute the command
    const result = await dequeue();
    return {
      isSuccess: true,
      timeInMs: new Date().getTime() - startTs,
    };
  } catch (error) {
    console.error("❌ Error query data:", error);
    return {
      isSuccess: false,
      timeInMs: new Date().getTime() - startTs,
    };
  }
}

async function main(numberProcesses = 1000) {
  let numberFailed = 0;
  let maxTime = 0;
  let maxTimeSuccess = 0;
  let maxTimeFailed = 0;
  let timeInMsArr = [];
  const promises = Array.from({ length: numberProcesses }, async (_, i) => {
    const result = await queryDynamoDB();

    timeInMsArr.push(result.timeInMs);

    if (!result.isSuccess) {
      numberFailed++;
    }

    if (result.timeInMs > maxTime) {
      maxTime = result.timeInMs;
    }

    if (result.isSuccess) {
      if (result.timeInMs > maxTimeSuccess) {
        maxTimeSuccess = result.timeInMs;
      }
    } else {
      if (result.timeInMs > maxTimeFailed) {
        maxTimeFailed = result.timeInMs;
      }
    }
  });

  await Promise.all(promises);

  console.log(`Number fail: ${numberFailed}`);
  console.log(`Max time success: ${maxTimeSuccess}`);

  return {
    numberFailed,
    maxTime,
    maxTimeSuccess,
    maxTimeFailed,
    timeInMsArr,
  };
}

main();
