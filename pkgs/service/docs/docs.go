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
                        "description": "Bad Request, past days less than 1, or invalid data market address",
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
        "service.SubmissionsRequest": {
            "type": "object",
            "properties": {
                "data_market_address": {
                    "type": "string"
                },
                "past_days": {
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
