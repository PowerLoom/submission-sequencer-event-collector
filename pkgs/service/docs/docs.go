// Package docs Code generated by swaggo/swag. DO NOT EDIT
package docs

import "github.com/swaggo/swag"

const docTemplate = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{escape .Description}}",
        "title": "{{.Title}}",
        "termsOfService": "http://yourterms.com",
        "contact": {
            "name": "API Support",
            "url": "http://www.yoursupport.com",
            "email": "support@example.com"
        },
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/batchCount": {
            "post": {
                "description": "Retrieves the total number of batches created within a specific epoch for a given data market address",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Batch Count"
                ],
                "summary": "Get total batch count",
                "parameters": [
                    {
                        "description": "Epoch data market request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EpochDataMarketRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_BatchCount"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/discardedSubmissions": {
            "post": {
                "description": "Retrieves the discarded submissions details within a specific epoch for a given data market address",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Discarded Submissions"
                ],
                "summary": "Get discarded submission details",
                "parameters": [
                    {
                        "description": "Epoch data market day request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EpochDataMarketDayRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_DiscardedSubmissionsAPIResponse"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, invalid day or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/eligibleNodesCount": {
            "post": {
                "description": "Retrieves the total count of eligible nodes and optionally their corresponding slotIDs (controlled by the includeSlotDetails query param) for a specified data market address and day",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Eligible Nodes"
                ],
                "summary": "Get eligible nodes count for a specific day",
                "parameters": [
                    {
                        "type": "boolean",
                        "description": "Set to true to include slotIDs in the response",
                        "name": "includeSlotDetails",
                        "in": "query"
                    },
                    {
                        "description": "Eligible nodes count payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EligibleNodesCountRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_EligibleNodes"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., day \u003c 1 or day \u003e current day, invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/eligibleNodesCountPastDays": {
            "post": {
                "description": "Retrieves the total count of eligible nodes along with their corresponding slotIDs for a specified data market address across a specified number of past days",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Eligible Nodes"
                ],
                "summary": "Get eligible nodes count for past days",
                "parameters": [
                    {
                        "description": "Eligible nodes count past days payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EligibleNodesPastDaysRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_ResponseArray-service_EligibleNodes"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., past days \u003c 1 or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/eligibleSlotSubmissionCount": {
            "post": {
                "description": "Retrieves the submission counts of all eligible slotIDs within a specific epoch for a given data market address",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Eligible Submission Count"
                ],
                "summary": "Get the submission counts of all eligible slotIDs",
                "parameters": [
                    {
                        "description": "Epoch data market day request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EpochDataMarketDayRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_EligibleSubmissionCountsResponse"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, invalid day or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/epochSubmissionDetails": {
            "post": {
                "description": "Retrieves the submission count and details of all submissions for a specific epoch and data market address",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Submissions"
                ],
                "summary": "Get epoch submission details",
                "parameters": [
                    {
                        "description": "Epoch data market request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EpochDataMarketRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_EpochSubmissionSummary"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/lastSimulatedSubmission": {
            "post": {
                "description": "Retrieves the last time a simulation submission was received for a given data market address and slotID",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Submissions"
                ],
                "summary": "Get the last time a simulation submission was received",
                "parameters": [
                    {
                        "description": "Data market address and slotID request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.SlotIDInDataMarketRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-string"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., invalid slotID or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "500": {
                        "description": "Internal Server Error: Failed to fetch last simulated submission",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/lastSnapshotSubmission": {
            "post": {
                "description": "Retrieves the last time a snapshot submission against a released epoch was received for a given data market address and slotID",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Submissions"
                ],
                "summary": "Get the last time a snapshot submission against a released epoch was received",
                "parameters": [
                    {
                        "description": "Data market address and slotID request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.SlotIDInDataMarketRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-string"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., invalid slotID or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "500": {
                        "description": "Internal Server Error: Failed to fetch last snapshot submission",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "/totalSubmissions": {
            "post": {
                "description": "Retrieves eligible and total submission counts for a specific data market address across a specified number of past days",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Submissions"
                ],
                "summary": "Get eligible and total submissions count",
                "parameters": [
                    {
                        "description": "Submissions request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.SubmissionsRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/service.Response-service_ResponseArray-service_DailySubmissions"
                        }
                    },
                    "400": {
                        "description": "Bad Request: Invalid input parameters (e.g., past days \u003c 1, invalid slotID or invalid data market address)",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "401": {
                        "description": "Unauthorized: Incorrect token",
                        "schema": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    },
    "definitions": {
        "service.BatchCount": {
            "type": "object",
            "properties": {
                "totalBatches": {
                    "type": "integer"
                }
            }
        },
        "service.DailySubmissions": {
            "type": "object",
            "properties": {
                "day": {
                    "type": "integer"
                },
                "eligibleSubmissions": {
                    "type": "integer"
                },
                "totalSubmissions": {
                    "type": "integer"
                }
            }
        },
        "service.DiscardedSubmissionDetails": {
            "type": "object",
            "properties": {
                "discardedSubmissionCount": {
                    "type": "integer"
                },
                "discardedSubmissions": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "mostFrequentSnapshotCID": {
                    "type": "string"
                }
            }
        },
        "service.DiscardedSubmissionDetailsResponse": {
            "type": "object",
            "properties": {
                "details": {
                    "$ref": "#/definitions/service.DiscardedSubmissionDetails"
                },
                "projectID": {
                    "type": "string"
                }
            }
        },
        "service.DiscardedSubmissionsAPIResponse": {
            "type": "object",
            "properties": {
                "projects": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/service.DiscardedSubmissionDetailsResponse"
                    }
                }
            }
        },
        "service.EligibleNodes": {
            "type": "object",
            "properties": {
                "day": {
                    "type": "integer"
                },
                "eligibleNodesCount": {
                    "type": "integer"
                },
                "slotIDs": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        },
        "service.EligibleNodesCountRequest": {
            "type": "object",
            "properties": {
                "dataMarketAddress": {
                    "type": "string"
                },
                "day": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        },
        "service.EligibleNodesPastDaysRequest": {
            "type": "object",
            "properties": {
                "dataMarketAddress": {
                    "type": "string"
                },
                "pastDays": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        },
        "service.EligibleSubmissionCounts": {
            "type": "object",
            "properties": {
                "count": {
                    "type": "integer"
                },
                "slotID": {
                    "type": "integer"
                }
            }
        },
        "service.EligibleSubmissionCountsResponse": {
            "type": "object",
            "properties": {
                "eligibleSubmissionCounts": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/service.EligibleSubmissionCounts"
                    }
                }
            }
        },
        "service.EpochDataMarketDayRequest": {
            "type": "object",
            "properties": {
                "dataMarketAddress": {
                    "type": "string"
                },
                "day": {
                    "type": "integer"
                },
                "epochID": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        },
        "service.EpochDataMarketRequest": {
            "type": "object",
            "properties": {
                "dataMarketAddress": {
                    "type": "string"
                },
                "epochID": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        },
        "service.EpochSubmissionSummary": {
            "type": "object",
            "properties": {
                "epochSubmissionCount": {
                    "type": "integer"
                },
                "submissions": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/service.SubmissionDetails"
                    }
                }
            }
        },
        "service.InfoType-service_BatchCount": {
            "type": "object",
            "properties": {
                "response": {
                    "$ref": "#/definitions/service.BatchCount"
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-service_DiscardedSubmissionsAPIResponse": {
            "type": "object",
            "properties": {
                "response": {
                    "$ref": "#/definitions/service.DiscardedSubmissionsAPIResponse"
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-service_EligibleNodes": {
            "type": "object",
            "properties": {
                "response": {
                    "$ref": "#/definitions/service.EligibleNodes"
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-service_EligibleSubmissionCountsResponse": {
            "type": "object",
            "properties": {
                "response": {
                    "$ref": "#/definitions/service.EligibleSubmissionCountsResponse"
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-service_EpochSubmissionSummary": {
            "type": "object",
            "properties": {
                "response": {
                    "$ref": "#/definitions/service.EpochSubmissionSummary"
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-service_ResponseArray-service_DailySubmissions": {
            "type": "object",
            "properties": {
                "response": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/service.DailySubmissions"
                    }
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-service_ResponseArray-service_EligibleNodes": {
            "type": "object",
            "properties": {
                "response": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/service.EligibleNodes"
                    }
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.InfoType-string": {
            "type": "object",
            "properties": {
                "response": {
                    "type": "string"
                },
                "success": {
                    "type": "boolean"
                }
            }
        },
        "service.RequestSwagger": {
            "type": "object",
            "properties": {
                "deadline": {
                    "type": "integer"
                },
                "epochID": {
                    "type": "integer"
                },
                "projectID": {
                    "type": "string"
                },
                "slotID": {
                    "type": "integer"
                },
                "snapshotCID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_BatchCount": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_BatchCount"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_DiscardedSubmissionsAPIResponse": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_DiscardedSubmissionsAPIResponse"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_EligibleNodes": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_EligibleNodes"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_EligibleSubmissionCountsResponse": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_EligibleSubmissionCountsResponse"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_EpochSubmissionSummary": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_EpochSubmissionSummary"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_ResponseArray-service_DailySubmissions": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_ResponseArray-service_DailySubmissions"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-service_ResponseArray-service_EligibleNodes": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_ResponseArray-service_EligibleNodes"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.Response-string": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-string"
                },
                "requestID": {
                    "type": "string"
                }
            }
        },
        "service.SlotIDInDataMarketRequest": {
            "type": "object",
            "properties": {
                "dataMarketAddress": {
                    "type": "string"
                },
                "slotID": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        },
        "service.SnapshotSubmissionSwagger": {
            "type": "object",
            "properties": {
                "header": {
                    "type": "string"
                },
                "request": {
                    "$ref": "#/definitions/service.RequestSwagger"
                },
                "signature": {
                    "type": "string"
                }
            }
        },
        "service.SubmissionDetails": {
            "type": "object",
            "properties": {
                "submissionData": {
                    "$ref": "#/definitions/service.SnapshotSubmissionSwagger"
                },
                "submissionID": {
                    "type": "string"
                }
            }
        },
        "service.SubmissionsRequest": {
            "type": "object",
            "properties": {
                "dataMarketAddress": {
                    "type": "string"
                },
                "pastDays": {
                    "type": "integer"
                },
                "slotID": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "1.0",
	Host:             "{{API_Host}}",
	BasePath:         "/",
	Schemes:          []string{"https"},
	Title:            "Sequencer API Documentation",
	Description:      "Offers comprehensive documentation of endpoints for seamless interaction with the sequencer, enabling efficient data retrieval.",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}
