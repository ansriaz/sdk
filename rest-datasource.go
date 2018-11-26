package sdk

/*
   Copyright 2016 Alexander I.Grafov <grafov@gmail.com>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   ॐ तारे तुत्तारे तुरे स्व
*/

import (
	"encoding/json"
	"fmt"
)

// GetAllDatasources loads all datasources.
// It reflects GET /api/datasources API call.
func (r *Client) GetAllDatasources(oid uint) ([]Datasource, error) {
	var (
		raw  []byte
		ds   []Datasource
		code int
		err  error
	)
	if raw, code, err = r.get("api/datasources", nil, oid); err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("HTTP error %d: returns %s", code, raw)
	}
	err = json.Unmarshal(raw, &ds)
	return ds, err
}

// GetDatasource gets an datasource by ID.
// It reflects GET /api/datasources/:datasourceId API call.
func (r *Client) GetDatasource(id uint, oid uint) (Datasource, error) {
	var (
		raw  []byte
		ds   Datasource
		code int
		err  error
	)
	if raw, code, err = r.get(fmt.Sprintf("api/datasources/%d", id), nil, oid); err != nil {
		return ds, err
	}
	if code != 200 {
		return ds, fmt.Errorf("HTTP error %d: returns %s", code, raw)
	}
	err = json.Unmarshal(raw, &ds)
	return ds, err
}

// GetDatasourceByName gets an datasource by Name.
// It reflects GET /api/datasources/name/:datasourceName API call.
func (r *Client) GetDatasourceByName(name string, oid uint) (Datasource, error) {
	var (
		raw  []byte
		ds   Datasource
		code int
		err  error
	)
	if raw, code, err = r.get(fmt.Sprintf("api/datasources/name/%s", name), nil, oid); err != nil {
		return ds, err
	}
	if code != 200 {
		return ds, fmt.Errorf("HTTP error %d: returns %s", code, raw)
	}
	err = json.Unmarshal(raw, &ds)
	return ds, err
}

// CreateDatasource creates a new datasource.
// It reflects POST /api/datasources API call.
func (r *Client) CreateDatasource(ds Datasource, oid uint) (StatusMessage, error) {
	var (
		raw  []byte
		resp StatusMessage
		err  error
	)
	if raw, err = json.Marshal(ds); err != nil {
		return StatusMessage{}, err
	}
	if raw, _, err = r.post("api/datasources", nil, raw, oid); err != nil {
		return StatusMessage{}, err
	}
	if err = json.Unmarshal(raw, &resp); err != nil {
		return StatusMessage{}, err
	}
	return resp, nil
}

// UpdateDatasource updates a datasource from data passed in argument.
// It reflects PUT /api/datasources/:datasourceId API call.
func (r *Client) UpdateDatasource(ds Datasource, oid uint) (StatusMessage, error) {
	var (
		raw  []byte
		resp StatusMessage
		err  error
	)
	if raw, err = json.Marshal(ds); err != nil {
		return StatusMessage{}, err
	}
	if raw, _, err = r.put(fmt.Sprintf("api/datasources/%d", ds.ID), nil, raw, oid); err != nil {
		return StatusMessage{}, err
	}
	if err = json.Unmarshal(raw, &resp); err != nil {
		return StatusMessage{}, err
	}
	return resp, nil
}

// DeleteDatasource deletes an existing datasource by ID.
// It reflects DELETE /api/datasources/:datasourceId API call.
func (r *Client) DeleteDatasource(id uint, oid uint) (StatusMessage, error) {
	var (
		raw   []byte
		reply StatusMessage
		err   error
	)
	if raw, _, err = r.delete(fmt.Sprintf("api/datasources/%d", id), oid); err != nil {
		return StatusMessage{}, err
	}
	err = json.Unmarshal(raw, &reply)
	return reply, err
}

// DeleteDatasourceByName deletes an existing datasource by Name.
// It reflects DELETE /api/datasources/name/:datasourceName API call.
func (r *Client) DeleteDatasourceByName(name string, oid uint) (StatusMessage, error) {
	var (
		raw   []byte
		reply StatusMessage
		err   error
	)
	if raw, _, err = r.delete(fmt.Sprintf("api/datasources/name/%s", name), oid); err != nil {
		return StatusMessage{}, err
	}
	err = json.Unmarshal(raw, &reply)
	return reply, err
}

// GetDatasourceTypes gets all available plugins for the datasources.
// It reflects GET /api/datasources/plugins API call.
func (r *Client) GetDatasourceTypes(oid uint) (map[string]DatasourceType, error) {
	var (
		raw     []byte
		dsTypes = make(map[string]DatasourceType)
		code    int
		err     error
	)
	if raw, code, err = r.get("api/datasources/plugins", nil, oid); err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("HTTP error %d: returns %s", code, raw)
	}
	err = json.Unmarshal(raw, &dsTypes)
	return dsTypes, err
}
