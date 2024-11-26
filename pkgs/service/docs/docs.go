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
                        "description": "Batch count request payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.BatchCountRequest"
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
        "/eligibleNodesCount": {
            "post": {
                "description": "Retrieves the total count of eligible nodes along with their corresponding slot IDs for a specified data market address and epochID across a specified number of past days",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Eligible Nodes Count"
                ],
                "summary": "Get eligible nodes count",
                "parameters": [
                    {
                        "description": "Eligible nodes count payload",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/service.EligibleNodesRequest"
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
                        "description": "Bad Request: Invalid input parameters (e.g., past days \u003c 1, missing or invalid epochID, or invalid data market address)",
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
        "/eligibleSubmissions": {
            "post": {
                "description": "Retrieves eligible submission counts for a specific data market address across a specified number of past days",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Submissions"
                ],
                "summary": "Get eligible submissions",
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
        },
        "/totalSubmissions": {
            "post": {
                "description": "Retrieves total submission counts for a specific data market address across a specified number of past days",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "Submissions"
                ],
                "summary": "Get total submissions",
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
                "total_batches": {
                    "type": "integer"
                }
            }
        },
        "service.BatchCountRequest": {
            "type": "object",
            "properties": {
                "data_market_address": {
                    "type": "string"
                },
                "epoch_id": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
                }
            }
        },
        "service.DailySubmissions": {
            "type": "object",
            "properties": {
                "day": {
                    "type": "integer"
                },
                "submissions": {
                    "type": "integer"
                }
            }
        },
        "service.EligibleNodes": {
            "type": "object",
            "properties": {
                "day": {
                    "type": "integer"
                },
                "eligible_nodes_count": {
                    "type": "integer"
                },
                "slot_ids": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        },
        "service.EligibleNodesRequest": {
            "type": "object",
            "properties": {
                "data_market_address": {
                    "type": "string"
                },
                "epoch_id": {
                    "type": "integer"
                },
                "past_days": {
                    "type": "integer"
                },
                "token": {
                    "type": "string"
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
        "service.Response-service_BatchCount": {
            "type": "object",
            "properties": {
                "info": {
                    "$ref": "#/definitions/service.InfoType-service_BatchCount"
                },
                "request_id": {
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
                "request_id": {
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
                "request_id": {
                    "type": "string"
                }
            }
        },
        "service.SubmissionsRequest": {
            "type": "object",
            "properties": {
                "data_market_address": {
                    "type": "string"
                },
                "past_days": {
                    "type": "integer"
                },
                "slot_id": {
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
	Host:             "localhost:8080",
	BasePath:         "/",
	Schemes:          []string{},
	Title:            "My API Documentation",
	Description:      "This API handles submissions and provides Swagger documentation",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}
