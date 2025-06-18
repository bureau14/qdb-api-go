package qdb

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertiesHaveSystemProperties(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	apiName, err := h.GetProperties("api")
	require.NoError(t, err)

	apiVers, err := h.GetProperties("api_version")
	require.NoError(t, err)

	assert.Equal(t, "go", apiName)
	assert.Equal(t, GitHash, apiVers)
}

func TestPropertiesCannotUpdateSystemProperties(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	err1 := h.UpdateProperties("api", "fake")
	err2 := h.UpdateProperties("api_version", "fake")

	assert.Error(t, err1)
	assert.Error(t, err2)
}

func TestPropertiesPutEmptyPropertyFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	value := generateAlias(16)
	err := h.PutProperties("", value)
	assert.Error(t, err)
}

func TestPropertiesPutDuplicateFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	prop := generateAlias(16)
	value := generateAlias(16)
	err := h.PutProperties(prop, value)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.RemoveProperties(prop) })

	err = h.PutProperties(prop, value+"_1")
	assert.Error(t, err)
}

func TestPropertiesPutEmptyValueFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	prop := generateAlias(16)
	err := h.PutProperties(prop, "")
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidArgument, err)
}

func TestPropertiesGetPropertiesSuccess(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	prop := generateAlias(16)
	value := generateAlias(16)
	err := h.PutProperties(prop, value)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.RemoveProperties(prop) })

	returnedValue, err := h.GetProperties(prop)
	require.NoError(t, err)
	assert.Equal(t, value, returnedValue)
}

func TestPropertiesGetEmptyNameFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	_, err := h.GetProperties("")
	assert.Error(t, err)
}

func TestPropertiesRemovePropertiesSuccess(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	prop := generateAlias(16)
	value := generateAlias(16)
	err := h.PutProperties(prop, value)
	require.NoError(t, err)

	err = h.RemoveProperties(prop)
	require.NoError(t, err)
}

func TestPropertiesRemoveEmptyNameFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	err := h.RemoveProperties("")
	assert.Error(t, err)
}

func TestPropertiesRemoveSystemPropertiesFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	err := h.RemoveProperties("api")
	assert.Error(t, err)
	err = h.RemoveProperties("api_version")
	assert.Error(t, err)
}

func TestPropertiesRemoveAllKeepsSystem(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	err := h.RemoveAllProperties()
	require.NoError(t, err)

	apiName, err := h.GetProperties("api")
	require.NoError(t, err)
	apiVersion, err := h.GetProperties("api_version")
	require.NoError(t, err)
	assert.Equal(t, "go", apiName)
	assert.Equal(t, GitHash, apiVersion)
}

func TestPropertiesUpdatePropertySuccess(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	prop := generateAlias(16)
	value := generateAlias(16)
	err := h.PutProperties(prop, value)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.RemoveProperties(prop) })

	err = h.UpdateProperties(prop, value+"_new")
	require.NoError(t, err)
}

func TestPropertiesUpdateNonexistentPropertySuccess(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	prop := generateAlias(16)
	err := h.UpdateProperties(prop, "test")
	require.NoError(t, err)
}

func TestPropertiesUpdateEmptyPropertyFails(t *testing.T) {
	h := newTestHandle(t)
	defer h.Close()

	err := h.UpdateProperties("", "test")
	assert.Error(t, err)
}
