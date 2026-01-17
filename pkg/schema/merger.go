package schema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	json "github.com/json-iterator/go"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"

	"grapthway/pkg/common"
	"grapthway/pkg/crypto"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/router"
	"grapthway/pkg/util"
)

type PostPipelineExecutor interface {
	ExecutePostPipeline(pipeline *model.PipelineConfig, mainResolverResponse map[string]interface{}, originalContext context.Context) (map[string]interface{}, error)
}

type Merger struct {
	storage              model.Storage
	schema               atomic.Value
	router               *router.ServiceRouter
	subgraphSchemas      atomic.Value
	postPipelineExecutor PostPipelineExecutor
	nodeIdentity         *crypto.Identity
}

func NewMerger(storage model.Storage, nodeIdentity *crypto.Identity) *Merger {
	merger := &Merger{
		storage:      storage,
		router:       router.NewServiceRouter(),
		nodeIdentity: nodeIdentity,
	}
	merger.schema.Store(merger.getEmptySchema())
	merger.subgraphSchemas.Store(make(map[string]*graphql.Schema))
	go merger.ReloadSchema()
	return merger
}

func (m *Merger) SetPostPipelineExecutor(executor PostPipelineExecutor) {
	m.postPipelineExecutor = executor
}

func (m *Merger) createProxyResolver(compositeKey, fieldName string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		logEntry, _ := p.Context.Value("log").(*logging.LogEntry)
		subgraph, _ := p.Context.Value("subgraph").(string)
		if subgraph == "" {
			subgraph = "main"
		}

		developerAddress, serviceName, err := util.ParseCompositeKey(compositeKey)
		if err != nil {
			return nil, fmt.Errorf("internal resolver error: invalid service key '%s'", compositeKey)
		}

		instances, err := m.storage.GetService(developerAddress, serviceName, subgraph)
		if err != nil || len(instances) == 0 {
			instances, err = m.storage.GetService(developerAddress, serviceName, "")
			if err != nil || len(instances) == 0 {
				return nil, fmt.Errorf("service %s unavailable for subgraph %s or globally", compositeKey, subgraph)
			}
		}

		instance := m.router.PickInstance(instances)
		downstreamHeaders := m.prepareHeadersForDownstream(p)

		pipelineConfig, _ := m.storage.GetMiddlewarePipeline(developerAddress, fieldName)

		allPostSteps := []model.PipelineStep{}
		if pipelineConfig != nil {
			allPostSteps = append(allPostSteps, pipelineConfig.Post...)
		}
		if pCtx, ok := p.Context.Value("pipelineContext").(*model.ExecutionResult); ok && pCtx != nil {
			allPostSteps = append(allPostSteps, pCtx.PostSteps...)
		}

		var extraFields []string

		argTypes := make(map[string]graphql.Input)
		if p.Info.ParentType != nil {
			if parentObj, ok := p.Info.ParentType.(*graphql.Object); ok {
				if field, ok := parentObj.Fields()[p.Info.FieldName]; ok {
					for _, arg := range field.Args {
						argTypes[arg.Name()] = arg.Type
					}
				}
			}
		}

		query := buildDownstreamQuery(p, p.Info.FieldName, p.Args, extraFields, argTypes)
		files, _ := p.Context.Value("graphql_files").(map[string][]*multipart.FileHeader)

		signature, err := m.createRequestSignature(instance.URL, query, downstreamHeaders)
		if err != nil {
			log.Printf("ERROR: Failed to create request signature for %s: %v", compositeKey, err)
			return nil, fmt.Errorf("could not sign downstream request: %w", err)
		}
		downstreamHeaders.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
		downstreamHeaders.Set("X-Grapthway-Node-Address", m.nodeIdentity.Address)
		downstreamHeaders.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(m.nodeIdentity.PublicKey)))

		result, err := m.executeGraphQLRequest(instance.URL, query, downstreamHeaders, files, logEntry)
		if err != nil {
			return nil, err
		}

		if errs, ok := result["errors"].([]interface{}); ok && len(errs) > 0 {
			firstError, _ := errs[0].(map[string]interface{})
			msg, _ := firstError["message"].(string)
			return nil, fmt.Errorf("error from %s: %s", compositeKey, msg)
		}

		// *** FIX START ***
		// Gracefully handle responses where the data field is null, which is a valid GraphQL response.
		if result["data"] == nil {
			return nil, nil // Return nil, which GraphQL interprets as a null value for the field.
		}

		mainResponseData, ok := result["data"].(map[string]interface{})
		if !ok {
			// This now correctly flags responses where 'data' is present but not a JSON object (e.g., a string or array).
			return nil, fmt.Errorf("invalid data format from service %s: 'data' field is not an object", compositeKey)
		}
		// *** FIX END ***

		mainFieldResult, hasField := mainResponseData[fieldName]
		if !hasField {
			return nil, nil
		}

		if m.postPipelineExecutor != nil && len(allPostSteps) > 0 {
			mainFieldResultMap, ok := mainFieldResult.(map[string]interface{})
			if !ok {
				log.Printf("Warning: Main resolver for '%s' did not return an object, cannot merge enrichment data.", fieldName)
				return mainFieldResult, nil
			}

			postExecutionConfig := &model.PipelineConfig{Post: allPostSteps}
			enrichedData, err := m.postPipelineExecutor.ExecutePostPipeline(postExecutionConfig, mainFieldResultMap, p.Context)
			if err != nil {
				log.Printf("Error executing post-pipeline for %s: %v", fieldName, err)
			}
			return enrichedData, nil
		}
		return mainFieldResult, nil
	}
}

func (m *Merger) createRequestSignature(url, query string, headers http.Header) ([]byte, error) {
	var headerParts []string
	for key, values := range headers {
		if strings.ToLower(key) == "x-grapthway-signature" {
			continue
		}
		for _, value := range values {
			headerParts = append(headerParts, fmt.Sprintf("%s:%s", strings.ToLower(key), value))
		}
	}

	// This payload must exactly match what the service will verify
	bodyBytes, _ := json.Marshal(map[string]string{"query": query})
	digest := sha256.Sum256(bodyBytes)

	return m.nodeIdentity.Sign(digest[:])
}

func (m *Merger) prepareHeadersForDownstream(p graphql.ResolveParams) http.Header {
	headers := http.Header{}
	if originalHeaders, ok := p.Context.Value("headers").(http.Header); ok {
		for key, values := range originalHeaders {
			// Don't forward the original user signature, the node will add its own
			if strings.ToLower(key) == "x-grapthway-user-signature" {
				continue
			}
			for _, value := range values {
				headers.Add(key, value)
			}
		}
	}

	if pipelineResult, ok := p.Context.Value("pipelineContext").(*model.ExecutionResult); ok && pipelineResult != nil {
		for key, val := range pipelineResult.Context {
			headerKey := "X-Ctx-" + key
			jsonVal, err := json.Marshal(val)
			if err == nil {
				headers.Set(headerKey, string(jsonVal))
			} else {
				headers.Set(headerKey, fmt.Sprintf("%v", val))
			}
		}
	}
	return headers
}

func (m *Merger) ReloadSchema() {
	serviceSchemas, err := m.storage.GetAllSchemas()
	if err != nil || len(serviceSchemas) == 0 {
		log.Println("MERGER: No schemas found in storage. Activating empty schema.")
		m.schema.Store(m.getEmptySchema())
		m.subgraphSchemas.Store(make(map[string]*graphql.Schema))
		return
	}

	log.Printf("MERGER: Reloading schemas for %d services.", len(serviceSchemas))

	globalCtx, err := m.createGlobalBuildContext(serviceSchemas)
	if err != nil {
		log.Printf("MERGER: Failed to create global build context: %v", err)
		return
	}

	allServices := make(map[string]struct{})
	for compositeKey := range serviceSchemas {
		allServices[compositeKey] = struct{}{}
	}

	newUnifiedSchema := m.buildSchemaForScope("unified", allServices, globalCtx)
	m.schema.Store(newUnifiedSchema)
	m.subgraphSchemas.Store(make(map[string]*graphql.Schema))
	log.Printf("MERGER: Successfully activated new unified schema.")
}

func (m *Merger) groupServicesBySubgraph(allServiceInstances map[string][]router.ServiceInstance) (map[string]map[string]struct{}, error) {
	servicesBySubgraph := make(map[string]map[string]struct{})

	for compositeKey, instances := range allServiceInstances {
		for _, instance := range instances {
			if instance.Type != "graphql" {
				continue
			}

			subgraph := instance.Subgraph
			if subgraph == "" {
				subgraph = "main"
			}

			if _, ok := servicesBySubgraph[subgraph]; !ok {
				servicesBySubgraph[subgraph] = make(map[string]struct{})
			}
			servicesBySubgraph[subgraph][compositeKey] = struct{}{}
		}
	}
	return servicesBySubgraph, nil
}

func (m *Merger) executeGraphQLRequest(url, query string, headers http.Header, files map[string][]*multipart.FileHeader, logEntry *logging.LogEntry) (map[string]interface{}, error) {
	var req *http.Request
	var err error
	var reqBodyStr string

	if len(files) == 0 {
		reqBody := map[string]interface{}{"query": query}
		jsonBody, _ := json.Marshal(reqBody)
		reqBodyStr = string(jsonBody)
		req, err = http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header = headers
		req.Header.Set("Content-Type", "application/json")
	} else {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		operations := map[string]interface{}{"query": query, "variables": map[string]interface{}{"file": nil}}
		operationsJSON, _ := json.Marshal(operations)
		_ = writer.WriteField("operations", string(operationsJSON))
		mapData := map[string][]string{"0": {"variables.file"}}
		mapJSON, _ := json.Marshal(mapData)
		_ = writer.WriteField("map", string(mapJSON))
		reqBodyStr = fmt.Sprintf("multipart form data:\noperations: %s\nmap: %s", string(operationsJSON), string(mapJSON))
		if fileHeaders, ok := files["0"]; ok && len(fileHeaders) > 0 {
			fileHeader := fileHeaders[0]
			part, err := writer.CreateFormFile("0", fileHeader.Filename)
			if err != nil {
				return nil, err
			}
			file, err := fileHeader.Open()
			if err != nil {
				return nil, err
			}
			defer file.Close()
			if _, err := io.Copy(part, file); err != nil {
				return nil, err
			}
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		req, err = http.NewRequest("POST", url, body)
		if err != nil {
			return nil, err
		}
		req.Header = headers
		req.Header.Set("Content-Type", writer.FormDataContentType())
	}

	if logEntry != nil {
		logEntry.DownstreamRequest = logging.RequestDetails{
			Method:  "POST",
			URL:     url,
			Headers: req.Header.Clone(),
			Body:    reqBodyStr,
		}
	}

	client := common.GetHTTPClientWithTimeout(60 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		if logEntry != nil {
			logEntry.DownstreamResponse.StatusCode = -1
		}
		m.router.ReportConnectionFailure(url)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if logEntry != nil {
		logEntry.DownstreamResponse = logging.ResponseDetails{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header.Clone(),
			Body:       string(respBody),
		}
	}

	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	return result, nil
}

func (m *Merger) createStitchingResolver(stitch model.FieldStitch, argTypes map[string]graphql.Input) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		logEntry, _ := p.Context.Value("log").(*logging.LogEntry)
		walletAddress, _ := p.Context.Value("wallet_address").(string)

		parentSource, ok := p.Source.(map[string]interface{})
		if !ok {
			val := reflect.ValueOf(p.Source)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			if val.Kind() == reflect.Map {
				if converted, ok := val.Interface().(map[string]interface{}); ok {
					parentSource = converted
				} else {
					return nil, fmt.Errorf("stitching error: parent source is a map but not map[string]interface{}")
				}
			} else {
				return nil, fmt.Errorf("stitching error: parent source is not a map, but a %T", p.Source)
			}
		}
		resolverArgs := make(map[string]interface{})
		for resolverArg, parentField := range stitch.ArgsMapping {
			parentValue, exists := parentSource[parentField]
			if !exists {
				return nil, fmt.Errorf("stitching error: required parent field '%s' not found for stitching '%s'", parentField, stitch.ResolverField)
			}
			resolverArgs[resolverArg] = parentValue
		}

		developerAddress, serviceName, err := util.ParseCompositeKey(stitch.Service)
		if err != nil {
			return nil, fmt.Errorf("stitching error: invalid service key '%s'", stitch.Service)
		}

		instances, err := m.storage.GetService(developerAddress, serviceName, walletAddress)
		if err != nil || len(instances) == 0 {
			instances, err = m.storage.GetService(developerAddress, serviceName, "")
			if err != nil || len(instances) == 0 {
				return nil, fmt.Errorf("stitching service %s unavailable for wallet %s or globally", stitch.Service, walletAddress)
			}
		}
		instance := m.router.PickInstance(instances)
		downstreamHeaders := m.prepareHeadersForDownstream(p)
		parentJson, err := json.Marshal(p.Source)
		if err == nil {
			downstreamHeaders.Set("X-Stitched-User-Context", string(parentJson))
		}

		query := buildDownstreamQuery(p, stitch.ResolverField, resolverArgs, nil, argTypes)
		result, err := m.executeGraphQLRequest(instance.URL, query, downstreamHeaders, nil, logEntry)
		if err != nil {
			return nil, err
		}
		if errs, ok := result["errors"].([]interface{}); ok && len(errs) > 0 {
			firstError, _ := errs[0].(map[string]interface{})
			msg, _ := firstError["message"].(string)
			return nil, fmt.Errorf("error from stitching service %s: %s", stitch.Service, msg)
		}
		if data, ok := result["data"].(map[string]interface{}); ok {
			return data[stitch.ResolverField], nil
		}
		return nil, fmt.Errorf("invalid response format from stitching service %s", stitch.Service)
	}
}

func (m *Merger) createEmptySchema() { m.schema.Store(m.getEmptySchema()) }

type buildContext struct {
	docs             map[string]*ast.Document
	typeMap          map[string]graphql.Type
	stitchingConfigs map[string]model.StitchingConfig
	objectDefs       map[string][]*ast.ObjectDefinition
	inputObjectDefs  map[string][]*ast.InputObjectDefinition
}

func (m *Merger) createGlobalBuildContext(serviceSchemas map[string]string) (*buildContext, error) {
	ctx := &buildContext{
		docs:            make(map[string]*ast.Document),
		typeMap:         make(map[string]graphql.Type),
		objectDefs:      make(map[string][]*ast.ObjectDefinition),
		inputObjectDefs: make(map[string][]*ast.InputObjectDefinition),
	}
	m.consolidateASTs(ctx, serviceSchemas)
	stitchingConfigs, err := m.storage.GetAllStitchingConfigs()
	if err != nil {
		return nil, err
	}
	ctx.stitchingConfigs = stitchingConfigs
	m.createAllTypes(ctx)
	return ctx, nil
}

func (m *Merger) consolidateASTs(ctx *buildContext, serviceSchemas map[string]string) {
	for service, schemaStr := range serviceSchemas {
		if schemaStr == "" {
			continue
		}
		doc, err := parser.Parse(parser.ParseParams{Source: &source.Source{Body: []byte(schemaStr), Name: service}})
		if err != nil {
			log.Printf("Error parsing schema for %s: %v", service, err)
			continue
		}
		ctx.docs[service] = doc
		for _, def := range doc.Definitions {
			switch d := def.(type) {
			case *ast.ObjectDefinition:
				ctx.objectDefs[d.Name.Value] = append(ctx.objectDefs[d.Name.Value], d)
			case *ast.InputObjectDefinition:
				ctx.inputObjectDefs[d.Name.Value] = append(ctx.inputObjectDefs[d.Name.Value], d)
			}
		}
	}
}

func (m *Merger) createAllTypes(ctx *buildContext) {
	ctx.typeMap["String"] = graphql.String
	ctx.typeMap["Int"] = graphql.Int
	ctx.typeMap["Float"] = graphql.Float
	ctx.typeMap["Boolean"] = graphql.Boolean
	ctx.typeMap["ID"] = graphql.ID
	for _, doc := range ctx.docs {
		for _, def := range doc.Definitions {
			switch d := def.(type) {
			case *ast.EnumDefinition:
				if _, ok := ctx.typeMap[d.Name.Value]; !ok {
					enumValues := graphql.EnumValueConfigMap{}
					for _, val := range d.Values {
						enumValues[val.Name.Value] = &graphql.EnumValueConfig{Value: val.Name.Value}
					}
					ctx.typeMap[d.Name.Value] = graphql.NewEnum(graphql.EnumConfig{Name: d.Name.Value, Values: enumValues})
				}
			case *ast.ScalarDefinition:
				if _, ok := ctx.typeMap[d.Name.Value]; !ok {
					ctx.typeMap[d.Name.Value] = graphql.NewScalar(graphql.ScalarConfig{Name: d.Name.Value, Serialize: func(v interface{}) interface{} { return v }})
				}
			}
		}
	}
	for typeName, typeDefs := range ctx.objectDefs {
		if _, ok := ctx.typeMap[typeName]; !ok {
			name, defs, bCtx := typeName, typeDefs, ctx
			objConf := graphql.ObjectConfig{
				Name: name, Description: getDescription(defs[0].Description),
				Fields: (graphql.FieldsThunk)(func() graphql.Fields { return m.buildFieldsForObject(defs, bCtx) }),
			}
			ctx.typeMap[name] = graphql.NewObject(objConf)
		}
	}
	for typeName, typeDefs := range ctx.inputObjectDefs {
		if _, ok := ctx.typeMap[typeName]; !ok {
			name, defs, bCtx := typeName, typeDefs, ctx
			inputObjConf := graphql.InputObjectConfig{
				Name: name, Description: getDescription(defs[0].Description),
				Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap { return m.buildFieldsForInputObject(defs, bCtx) }),
			}
			ctx.typeMap[name] = graphql.NewInputObject(inputObjConf)
		}
	}
}

func (m *Merger) buildFieldsForObject(typeDefs []*ast.ObjectDefinition, ctx *buildContext) graphql.Fields {
	fields := graphql.Fields{}
	for _, typeDef := range typeDefs {
		for _, fieldDef := range typeDef.Fields {
			if _, ok := fields[fieldDef.Name.Value]; !ok {
				fields[fieldDef.Name.Value] = &graphql.Field{
					Type: m.parseFieldType(fieldDef.Type, ctx), Args: m.parseArguments(fieldDef.Arguments, ctx), Description: getDescription(fieldDef.Description),
				}
			}
		}
	}
	typeName := typeDefs[0].Name.Value
	for compositeKey, serviceConfig := range ctx.stitchingConfigs {
		if fieldStitches, ok := serviceConfig[typeName]; ok {
			for fieldName, stitch := range fieldStitches {
				if _, ok := fields[fieldName]; ok {
					continue
				}
				stitch.Service = compositeKey
				resolverFieldDef := m.findFieldDefinition(stitch.ResolverField, "Query", ctx.docs[stitch.Service])
				if resolverFieldDef == nil {
					log.Printf("Warning: Stitch resolver field '%s' not found in service '%s'.", stitch.ResolverField, stitch.Service)
					continue
				}

				parsedArgs := m.parseArguments(resolverFieldDef.Arguments, ctx)
				argTypes := make(map[string]graphql.Input)
				for name, config := range parsedArgs {
					argTypes[name] = config.Type
				}

				fields[fieldName] = &graphql.Field{
					Type:    m.parseFieldType(resolverFieldDef.Type, ctx),
					Args:    m.parseArguments(resolverFieldDef.Arguments, ctx),
					Resolve: m.createStitchingResolver(stitch, argTypes),
				}
			}
		}
	}
	return fields
}

func (m *Merger) buildFieldsForInputObject(typeDefs []*ast.InputObjectDefinition, ctx *buildContext) graphql.InputObjectConfigFieldMap {
	fields := graphql.InputObjectConfigFieldMap{}
	for _, typeDef := range typeDefs {
		for _, fieldDef := range typeDef.Fields {
			if _, ok := fields[fieldDef.Name.Value]; !ok {
				fields[fieldDef.Name.Value] = &graphql.InputObjectFieldConfig{
					Type: m.parseFieldType(fieldDef.Type, ctx), Description: getDescription(fieldDef.Description),
				}
			}
		}
	}
	return fields
}

func (m *Merger) buildSchemaForScope(scopeName string, servicesInScope map[string]struct{}, globalCtx *buildContext) *graphql.Schema {
	queryFields := m.buildRootFields(globalCtx, "Query", servicesInScope)
	mutationFields := m.buildRootFields(globalCtx, "Mutation", servicesInScope)
	if len(queryFields) == 0 {
		return m.getEmptySchema()
	}
	schemaConfig := graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{Name: "Query", Fields: queryFields}),
	}
	if len(mutationFields) > 0 {
		schemaConfig.Mutation = graphql.NewObject(graphql.ObjectConfig{Name: "Mutation", Fields: mutationFields})
	}
	var schemaTypes []graphql.Type
	for typeName, t := range globalCtx.typeMap {
		if typeName == "Query" || typeName == "Mutation" {
			continue
		}
		schemaTypes = append(schemaTypes, t)
	}
	schemaConfig.Types = schemaTypes
	mergedSchema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.Printf("Error creating merged schema for scope %s: %v", scopeName, err)
		return m.getEmptySchema()
	}
	log.Printf("Successfully built schema for scope: %s", scopeName)
	return &mergedSchema
}

func (m *Merger) buildRootFields(ctx *buildContext, rootTypeName string, servicesInScope map[string]struct{}) graphql.Fields {
	rootFields := graphql.Fields{}
	for compositeKey := range servicesInScope {
		doc, ok := ctx.docs[compositeKey]
		if !ok {
			continue
		}
		developerAddress, _, err := util.ParseCompositeKey(compositeKey)
		if err != nil {
			continue
		}
		for _, def := range doc.Definitions {
			if typeDef, ok := def.(*ast.ObjectDefinition); ok && typeDef.Name.Value == rootTypeName {
				for _, field := range typeDef.Fields {
					if _, exists := rootFields[field.Name.Value]; !exists {
						pipelineConfig, _ := m.storage.GetMiddlewarePipeline(developerAddress, field.Name.Value)
						if pipelineConfig != nil && pipelineConfig.IsInternal {
							log.Printf("MERGER: Skipping internal field '%s' from public schema.", field.Name.Value)
							continue
						}

						rootFields[field.Name.Value] = &graphql.Field{
							Type:    m.parseFieldType(field.Type, ctx),
							Args:    m.parseArguments(field.Arguments, ctx),
							Resolve: m.createProxyResolver(compositeKey, field.Name.Value),
						}
					}
				}
			}
		}
	}
	return rootFields
}

func (m *Merger) findFieldDefinition(fieldName, typeName string, doc *ast.Document) *ast.FieldDefinition {
	if doc == nil {
		return nil
	}
	for _, def := range doc.Definitions {
		if typeDef, ok := def.(*ast.ObjectDefinition); ok && typeDef.Name.Value == typeName {
			for _, field := range typeDef.Fields {
				if field.Name.Value == fieldName {
					return field
				}
			}
		}
	}
	return nil
}

func (m *Merger) parseFieldType(fieldType ast.Type, ctx *buildContext) graphql.Type {
	switch t := fieldType.(type) {
	case *ast.Named:
		if gqlType, exists := ctx.typeMap[t.Name.Value]; exists {
			return gqlType
		}
		log.Printf("FATAL: Type '%s' was not pre-discovered. This indicates a bug in the build process.", t.Name.Value)
		return graphql.String
	case *ast.List:
		return graphql.NewList(m.parseFieldType(t.Type, ctx))
	case *ast.NonNull:
		if innerType := m.parseFieldType(t.Type, ctx); innerType != nil {
			return graphql.NewNonNull(innerType)
		}
		return nil
	default:
		return graphql.String
	}
}

func (m *Merger) parseArguments(args []*ast.InputValueDefinition, ctx *buildContext) graphql.FieldConfigArgument {
	arguments := graphql.FieldConfigArgument{}
	for _, arg := range args {
		if argType := m.parseFieldType(arg.Type, ctx); argType != nil {
			if inputType, ok := argType.(graphql.Input); ok {
				arguments[arg.Name.Value] = &graphql.ArgumentConfig{Type: inputType}
			}
		}
	}
	return arguments
}

func buildDownstreamQuery(p graphql.ResolveParams, fieldName string, args map[string]interface{}, extraFields []string, argTypes map[string]graphql.Input) string {
	argsStr := buildArgumentsString(args, argTypes)
	selectionSet := buildSelectionSet(p, p.Info.FieldASTs, extraFields)
	operation := p.Info.Operation.GetOperation()
	if operation == "" {
		operation = "query"
	}
	return fmt.Sprintf("%s { %s%s %s }", operation, fieldName, argsStr, selectionSet)
}

func buildArgumentsString(args map[string]interface{}, argTypes map[string]graphql.Input) string {
	if len(args) == 0 {
		return ""
	}
	var argParts []string
	for key, val := range args {
		argParts = append(argParts, fmt.Sprintf("%s: %s", key, util.ValueToString(val, argTypes[key])))
	}
	return fmt.Sprintf("(%s)", strings.Join(argParts, ", "))
}

func buildSelectionSet(p graphql.ResolveParams, fieldASTs []*ast.Field, extraFields []string) string {
	if len(fieldASTs) == 0 || fieldASTs[0].SelectionSet == nil {
		if len(extraFields) > 0 {
			return formatExtraFields(extraFields)
		}
		return "{ __typename }"
	}

	var selections []string
	for _, sel := range fieldASTs[0].SelectionSet.Selections {
		if field, ok := sel.(*ast.Field); ok {
			if field.SelectionSet != nil {
				selections = append(selections, fmt.Sprintf("%s %s", field.Name.Value, buildNestedSelectionSet(field)))
			} else {
				selections = append(selections, field.Name.Value)
			}
		}
	}

	for _, field := range extraFields {
		if !contains(selections, field) {
			if strings.Contains(field, ".") {
				selections = append(selections, formatNestedField(field))
			} else {
				selections = append(selections, field)
			}
		}
	}

	return fmt.Sprintf("{ %s }", strings.Join(selections, " "))
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func formatNestedField(path string) string {
	parts := strings.Split(path, ".")
	result := parts[len(parts)-1]
	for i := len(parts) - 2; i >= 0; i-- {
		result = fmt.Sprintf("%s { %s }", parts[i], result)
	}
	return result
}

func formatExtraFields(fields []string) string {
	var selections []string
	for _, field := range fields {
		if strings.Contains(field, ".") {
			selections = append(selections, formatNestedField(field))
		} else {
			selections = append(selections, field)
		}
	}
	return fmt.Sprintf("{ %s }", strings.Join(selections, " "))
}

func buildNestedSelectionSet(field *ast.Field) string {
	if field.SelectionSet == nil {
		return ""
	}
	var selections []string
	for _, sel := range field.SelectionSet.Selections {
		if f, ok := sel.(*ast.Field); ok {
			if f.SelectionSet != nil {
				selections = append(selections, fmt.Sprintf("%s %s", f.Name.Value, buildNestedSelectionSet(f)))
			} else {
				selections = append(selections, f.Name.Value)
			}
		}
	}
	return fmt.Sprintf("{ %s }", strings.Join(selections, " "))
}

func getDescription(node *ast.StringValue) string {
	if node != nil {
		return node.Value
	}
	return ""
}

func (m *Merger) getEmptySchema() *graphql.Schema {
	s, _ := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query", Fields: graphql.Fields{"_empty": &graphql.Field{Type: graphql.String}},
		}),
	})
	return &s
}

func (m *Merger) GetSchema(subgraph string) *graphql.Schema {
	s := m.schema.Load()
	if schema, ok := s.(*graphql.Schema); ok && schema != nil {
		return schema
	}
	return m.getEmptySchema()
}

func (m *Merger) GetSubgraphSchema(subgraph string) *graphql.Schema {
	return m.GetSchema(subgraph)
}
