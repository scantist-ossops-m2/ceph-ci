// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LUKSEncryptionFormat.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/compat.h"
#include "librbd/crypto/luks/FormatRequest.h"
#include "librbd/crypto/luks/LoadRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::luks::LUKSEncryptionFormat:: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace crypto {
namespace luks {

template <typename I>
LUKSEncryptionFormat<I>::LUKSEncryptionFormat(
        encryption_algorithm_t alg,
        std::string&& passphrase) : m_passphrase(std::move(passphrase)),
                                    m_alg(alg) {
}

template <typename I>
LUKSEncryptionFormat<I>::LUKSEncryptionFormat(
        std::string&& passphrase) : m_passphrase(std::move(passphrase)) {
}

template <typename I>
LUKSEncryptionFormat<I>::~LUKSEncryptionFormat() {
  ceph_memzero_s(
          &m_passphrase[0], m_passphrase.capacity(), m_passphrase.size());
}

template <typename I>
void LUKSEncryptionFormat<I>::format(I* image_ctx, Context* on_finish) {
  if (get_format() == RBD_ENCRYPTION_FORMAT_LUKS) {
    lderr(image_ctx->cct) << "explicit LUKS version required for format"
                          << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto req = luks::FormatRequest<I>::create(
          image_ctx, get_format(), m_alg, std::move(m_passphrase), &m_crypto,
          on_finish, false);
  req->send();
}

template <typename I>
void LUKSEncryptionFormat<I>::load(I* image_ctx, Context* on_finish) {
  auto req = luks::LoadRequest<I>::create(
          image_ctx, RBD_ENCRYPTION_FORMAT_LUKS, std::move(m_passphrase),
          &m_crypto, on_finish);
  req->send();
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::LUKSEncryptionFormat<librbd::ImageCtx>;
template class librbd::crypto::luks::LUKS1EncryptionFormat<librbd::ImageCtx>;
template class librbd::crypto::luks::LUKS2EncryptionFormat<librbd::ImageCtx>;
