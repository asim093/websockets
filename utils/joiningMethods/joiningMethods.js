function getJoiningMethod(type = "latest") {
  const isAll = type === "all";

  const lookupStage = {
    $lookup: {
      from: "DesignVersion",
      let: { designId: "$_id" },
      pipeline: [
        {
          $match: {
            $expr: { $eq: ["$designId", "$$designId"] },
          },
        },
        ...(isAll
          ? [
              {
                $match: {
                  $or: [
                    { internalStatus: "Awaiting Approval" },          // explicitly "New"
                    { internalStatus: { $exists: false } }, // field doesn’t exist
                    { internalStatus: null },           // or is null
                  ],
                },
              },
            ]
          : [
              { $sort: { version: -1 } },
              { $limit: 1 },
            ]),
      ],
      as: "latestVersion",
    },
  };

  const unwindStage = isAll
    ? null 
    : {
        $unwind: {
          path: "$latestVersion",
          preserveNullAndEmptyArrays: true,
        },
      };

  const projectStage = {
    $project: {
      name: 1,
      createdBy: 1,
      requestId: 1,
      createdAt: 1,
      updatedAt: 1,
      ...(isAll
        ? { allVersions: "$latestVersion" }
        : {
            latestVersionId: "$latestVersion._id",
            latestImage: "$latestVersion.imageLink",
            latestVersionNumber: "$latestVersion.version",
          }),
    },
  };

  return {
    $lookup: lookupStage,
    ...(unwindStage ? { $unwind: unwindStage } : {}),
    $project: projectStage,
  };
}

module.exports = getJoiningMethod;
